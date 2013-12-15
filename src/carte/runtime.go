package carte

import (
	"time"
	"fmt"
	"net/rpc"
)

const (
	CHECK_TIMES = 3
	TIME_SLEEP = 5
	WAITING_FOR_READY = 3
)

type Runtime struct {
	cfg *Config
	sch *Scheduler
	rpcServer *RPCServer
	
	ready bool
	fch chan bool
}

func NewRuntime(program HEdgeProgram) *Runtime {
	cfg := NewConfig()
	return &Runtime{cfg, NewScheduler(cfg, program), nil, false, nil}
}

func (runtime *Runtime) Init(idx int, hgraphPath string, initDataPath string, configPath string) {
	runtime.cfg.currentIdx = idx
	runtime.cfg.LoadFromFile(configPath)
	
	runtime.sch.env.LoadFromFile(hgraphPath)
	runtime.sch.env.LoadInitFile(initDataPath)
	
	runtime.rpcServer = NewRPCServer(runtime.cfg.ports[idx])
	runtime.rpcServer.Init(runtime) 
	
	runtime.fch = make(chan bool)
	
	runtime.ready = true
}

func (runtime *Runtime) IsTempFinished() bool {
	return runtime.sch.queue.Size() == 0
}

func (runtime *Runtime) IsReady() bool {
	return runtime.ready
}

func (runtime *Runtime) Monite() {
	finished := false
	times := 0
	start := time.Now()
	for !finished {
		size := runtime.sch.cfg.hostsCount
		fch := make(chan bool, size)
		for i:=0; i<size; i++ {
			if i == runtime.cfg.currentIdx {
				fmt.Printf("Rest count in Scheduler: %d, sec: %.3f\n", 
						   runtime.sch.queue.Size(), 
						   (time.Now().Sub(start).Seconds()))
				fch <- runtime.IsTempFinished()
			} else {
				idx := i
				go func() {
					host, port := runtime.sch.cfg.hosts[idx], runtime.sch.cfg.ports[idx]
					addr := fmt.Sprintf("%s:%d", host, port)
					client, err := rpc.DialHTTP("tcp", addr)
					if err != nil {
						fmt.Printf("Cannot connect to other instance\n")
						fch <- true
						return
					}
					isReady := false
					err = client.Call("RPCRuntimeProxy.IsTempFinished", &EmptyArgs{}, &isReady)
					if err != nil {
						panic(err)
					}
					fch <- isReady
				}()
			}
		}
		var loopFinished interface{} = nil
		for i:=0; i<size; i++ {
			if loopFinished == nil {
				loopFinished = <-fch
			} else {
				loopFinished = loopFinished.(bool) && <-fch
			}
		}
		
		if loopFinished.(bool) {
			fmt.Printf("Check finished at sec: %.3f\n", (time.Now().Sub(start).Seconds()))
			
			if times < CHECK_TIMES {
				times++
			} else {
				finished = true
			}
		} else {
			times = 0
		}
		time.Sleep(TIME_SLEEP * time.Second)
	}
	
	runtime.fch <- true
}

func (runtime *Runtime) getReady() {
	hostSize := runtime.cfg.hostsCount
	rdyCh := make(chan bool, hostSize)
	for i:=0; i<hostSize; i++ {
		if i == runtime.cfg.currentIdx {
			go func() {
				for !runtime.IsReady() {
					time.Sleep(WAITING_FOR_READY * time.Second)
				}
				rdyCh <- true
			}()
		} else {
			idx := i
			go func() {
				host, port := runtime.sch.cfg.hosts[idx], runtime.sch.cfg.ports[idx]
				ready := false
				for !ready {
					addr := fmt.Sprintf("%s:%d", host, port)
					client, err := rpc.DialHTTP("tcp", addr)
					if err != nil {
						ready = false
					} else {
						err = client.Call("RPCRuntimeProxy.IsReady", EmptyArgs{}, &ready)
						if err != nil {
							panic(err)
						}
					}
					if !ready {
						time.Sleep(WAITING_FOR_READY * time.Second)
					}
				}
				fmt.Printf("Ready\n")
				rdyCh <- true
			}()
		}
	}
	
	for i:=0; i<runtime.sch.cfg.hostsCount; i++ {
		<-rdyCh
	}
}

func (runtime *Runtime) Start() {
	fmt.Printf("Waiting for getting ready\n")
	runtime.getReady()
	fmt.Printf("Ready! Scheduler is ready to run\n")

	go runtime.sch.Start()
	go runtime.Monite()
	
	defer runtime.sch.Stop()
	defer runtime.rpcServer.Close()
	
	<- runtime.fch
	
	fmt.Printf("Start to output file...\n")
	runtime.sch.Output()
	fmt.Printf("Output finished\n")
	
	fmt.Printf("Carte Runtime Finished\n")
}