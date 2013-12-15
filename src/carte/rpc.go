package carte

import (
	"net"
	"net/rpc"
	"net/http"
	"strconv"
//	"fmt"
)

type RPCSchProxy struct {
	sch *Scheduler
}

func NewRPCSchProxy(sch *Scheduler) *RPCSchProxy {
	return &RPCSchProxy{sch}
}

func (proxy *RPCSchProxy) MapReduce(eid uint64, res *map[interface{}]interface{}) error {
//	fmt.Println("mr started")
	c := NewContextImpl(proxy.sch.env.edgeMap[eid], proxy.sch)
	proxy.sch.Map(c)
	*res = proxy.sch.Reduce(c)
//	fmt.Println("mr finished")
	return nil
} 

type ApplyArgs struct {
	Eid uint64
	M map[interface{}]interface{}
}

type EmptyArgs struct {
}

type RPCEdgeInfo struct {
	Id uint64
	Location int
	Degree int
}

func (proxy *RPCSchProxy) Apply(args *ApplyArgs, res *bool) error {
	c := NewContextImpl(proxy.sch.env.edgeMap[args.Eid], proxy.sch)
	proxy.sch.Apply(c, args.M)
	*res = true
	return nil
}

func (proxy *RPCSchProxy) Notify(eid uint64, res *bool) error {
	c := NewContextImpl(proxy.sch.env.edgeMap[eid], proxy.sch)
	proxy.sch.Notify(c)
	*res = true
	return nil
}

func (proxy *RPCSchProxy) Activate(eid uint64, res *bool) error {
	d := proxy.sch.env.edgeMap[eid].degree
	proxy.sch.LocalActivate(HEdgeInfo{eid, proxy.sch.cfg.currentIdx, d})
	*res = true
	return nil
}

func (proxy *RPCSchProxy) InitFinish(idx int, res *bool) error {
	proxy.sch.InitFinish(idx)
	*res = true
	return nil
}

type RPCRuntimeProxy struct {
	runtime *Runtime
}

func NewRPCRuntimeProxy(runtime *Runtime) *RPCRuntimeProxy {
	return &RPCRuntimeProxy{runtime}
}

func (proxy *RPCRuntimeProxy) IsTempFinished(args *EmptyArgs, tempFinished *bool) error {
	*tempFinished = proxy.runtime.IsTempFinished()
	return nil
}

func (proxy *RPCRuntimeProxy) IsReady(args *EmptyArgs, isReady *bool) error {
	*isReady = proxy.runtime.IsReady()
	return nil
}

type RPCServer struct {
	port int
	lis net.Listener
}

func NewRPCServer(port int) *RPCServer {
	return &RPCServer{port, nil}
//	return &RPCServer{port}
}

func (server *RPCServer) Init(runtime *Runtime) {
	rpc.Register(NewRPCSchProxy(runtime.sch))
	rpc.Register(NewRPCRuntimeProxy(runtime))
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+strconv.Itoa(server.port))
	if e != nil {
		panic("Cannot start RPC server")
	}
	server.lis = l
	go http.Serve(l, nil)
}

func (server *RPCServer) Close() {
	server.lis.Close()
}