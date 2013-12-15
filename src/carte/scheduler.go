package carte

import (
	"fmt"
	"net/rpc"
	"sync"
)

const (
	MAX_ROUTINES = 10
)

type Scheduler struct {
	env *Env
	queue *Queue
	cfg *Config
	program HEdgeProgram
	
	initCh chan bool
	fch chan bool
	finish bool
}

func NewScheduler(cfg *Config, program HEdgeProgram) *Scheduler {
	s := new(Scheduler)
	s.env = NewEnv(cfg)
	s.queue = NewQueue()
	s.cfg = cfg
	s.program = program
	
	s.initCh = make(chan bool, s.cfg.hostsCount)
	s.fch = make(chan bool)
	s.finish = false
	return s
}

func (sch *Scheduler) NotifyAll() {
	for _, e := range(sch.env.edgeMap) {
		if e.isMaster {
			sch.queue.Enqueue(e)
		}
	}
}

func (sch *Scheduler) Map(c *ContextImpl) {
	for _, v := range(c.edge.vertices) {
		sch.program.Map(c, v)
	}
}

func (sch *Scheduler) Reduce(c *ContextImpl) map[interface{}]interface{} {
	m := make(map[interface{}]interface{})
	for k, v := range(c.edge.mrKV) {
		m[k] = sch.program.Reduce(c, v)
	}
	return m
}

func (sch *Scheduler) Apply(c *ContextImpl, m map[interface{}]interface{}) {
	for _, v := range(c.edge.vertices) {
		v.l.Lock()
		sch.program.Apply(c, v, m)
		v.l.Unlock()
	}
	sch.program.Apply(c, c.edge, m)
	c.edge.ClearMR()
}

func (sch *Scheduler) Notify(c *ContextImpl) {
	for _, v := range(c.edge.vertices) {
		sch.program.Notify(c, v)
	}
	if c.edge.isMaster {
		sch.program.Notify(c, c.edge)
	}
}

func (sch *Scheduler) LocalActivate(edgeInfo HEdgeInfo) {
	if e, ok := sch.env.edgeMap[edgeInfo.id]; ok {
		if !e.isMaster {
			panic(fmt.Sprintf("Edge: %d is activated local, but not master", edgeInfo.id))
		}
		sch.queue.Enqueue(e)
	} else {
		panic(fmt.Sprintf("Edge: %d is local, but not exists", edgeInfo.id))
	}
}

func (sch *Scheduler) Activate(edgeInfo HEdgeInfo) {
	if edgeInfo.location == sch.cfg.currentIdx {
		sch.LocalActivate(edgeInfo)
	} else {
		// Remote call
		host, port := sch.cfg.hosts[edgeInfo.location], sch.cfg.ports[edgeInfo.location]
		addr := fmt.Sprintf("%s:%d", host, port)
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			panic(err)
		}
		var res bool
		err = client.Call("RPCSchProxy.Activate", edgeInfo.id, &res)
		if err != nil {
			panic(err)
		}
	}
}

func (sch *Scheduler) Output() {
	enCh := make(chan Entity)
	
	go sch.program.Output(enCh)
	
	for _, v := range(sch.env.vertexMap) {
		enCh <- v
	}
	for _, e := range(sch.env.edgeMap) {
		enCh <- e
	}
	enCh <- nil
}

func (sch *Scheduler) Run(edge *HEdge, init bool) {
	ctx := NewContextImpl(edge, sch)
	length := 1 + len(edge.remoteInfos)
	
	mrch := make(chan map[interface{}]interface{}, length)
	go func() {
		sch.Map(ctx)
		mrch <- sch.Reduce(ctx)
	}()
	for i:=0; i<length-1; i++ {
		idx := i
		go func() {
			info := edge.remoteInfos[idx]
			host := sch.cfg.hosts[info.location]
			port := sch.cfg.ports[info.location]
			addr := fmt.Sprintf("%s:%d", host, port)
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				panic(err)
			}
			
			var res map[interface{}]interface{}
			err = client.Call("RPCSchProxy.MapReduce", edge.id, &res)
			if err != nil {
				panic(err)
			}
			client.Close()
			mrch <- res
		}()
	}
	m := make(map[interface{}]interface{})
	l := new(sync.Mutex)
	for i:=0; i<length; i++ {
		sMap := <-mrch
		for k, v := range(sMap) {
			l.Lock()
			if mv, ok := m[k]; ok {
				m[k] = sch.program.Reduce(ctx, []interface{}{v, mv})
			} else {
				m[k] = v
			}
			l.Unlock()
		}
	}
		
	finished := make(chan bool, length)
	go func() {
		sch.Apply(ctx, m)
		if !init {
			sch.Notify(ctx)
		}
		finished <- true
	}()
	for i:=0; i<length-1; i++ {
		idx := i
		go func() {
			info := edge.remoteInfos[idx]
			host := sch.cfg.hosts[info.location]
			port := sch.cfg.ports[info.location]
			addr := fmt.Sprintf("%s:%d", host, port)
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				panic(err)
			}
			var res bool
			err = client.Call("RPCSchProxy.Apply", &ApplyArgs{edge.id, m}, &res)
			if err != nil {
				panic(err)
			}
			if !init {
				err = client.Call("RPCSchProxy.Notify", edge.id, &res)
				if err != nil {
					panic(err)
				}
			}
			finished <- true
		}()
	}
	for i:=0; i<length; i++ {
		<-finished
	}
}

func (sch *Scheduler) dataInit(routines chan bool) {
	// init local
	go func() {
		localInitCh := make(chan bool, 1)
		localRest := sch.queue.Size()
		for sch.queue.Size() > 0 {
			routines <- true
			go func() {
				ele := sch.queue.Dequeue()
				if ele != nil {
					e:= ele.Value.(*HEdge)
					sch.Run(e, true)
					localRest--
					if localRest == 0 {
						localInitCh <- true
					}
				}
				<-routines
			}()
		}
		<-localInitCh
		sch.initCh <- true
		fmt.Printf("Local init finished\n")
		
		for i := 0; i < sch.cfg.hostsCount; i++ {
			idx := i
			if idx == sch.cfg.currentIdx {
				continue
			}
			go func() {
				host := sch.cfg.hosts[idx]
				port := sch.cfg.ports[idx]
				addr := fmt.Sprintf("%s:%d", host, port)
				client, err := rpc.DialHTTP("tcp", addr)
				if err != nil {
					panic(err)
				}
				var res bool
				err = client.Call("RPCSchProxy.InitFinish", sch.cfg.currentIdx, &res)
				if err != nil {
					panic(err)
				}
			}()
		}
	}()
	
	for i := 0; i < sch.cfg.hostsCount; i++ {
		<-sch.initCh
	}
}

func (sch *Scheduler) InitFinish(hostIdx int) {
	sch.initCh <- true
}

func (sch *Scheduler) Start() {
	// call to init
	sch.NotifyAll()

	routines := make(chan bool, MAX_ROUTINES)
	
	if sch.program.NeedInit() {
		fmt.Printf("Scheduler init started\n")
		sch.dataInit(routines)
		fmt.Printf("Scheduler init finished\n")
		// call to perform computing
		sch.NotifyAll()
	}
	
	for !sch.finish {
		routines <- true
		go func() {
			ele := sch.queue.Dequeue()
			if ele != nil {
				e := ele.Value.(*HEdge)
				sch.Run(e, false)
			}
			<-routines
		}()
	}
	<- sch.fch
	fmt.Printf("Scheduler finished\n")
	
}

func (sch *Scheduler) Stop() {
	sch.finish = true
	sch.fch <- true
}