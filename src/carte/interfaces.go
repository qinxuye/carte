package carte

import (

)

type Entity interface {
	GetProperty(interface{}) interface{}
	SetProperty(key interface{}, value interface{})
	HasProperty(key interface{}) bool
	RemoveProperty(key interface{})
	GetPropertyKeys() []interface{}
	GetPropertyKeysWithPrefix(prefix string) []interface{}
	GetId() uint64
	TypeName() string
}

type ContextObj interface {
	Emit(k interface{}, v interface{})
	
	Entity
	Degree() int
	GetDegree(entity Entity) int
	GetInfo() *HEdgeInfo
}

type Context interface {
	GetContextObj() ContextObj
	CollectLocalNbors(v Entity) []Entity
	CollectNborIDs(v Entity) []uint64
	Activate(e HEdgeInfo)
}

type HEdgeProgram interface {
	NeedInit() bool
	Map(c Context, v Entity)
	Reduce(c Context, values []interface{}) interface{}
	Apply(c Context, entry Entity, m map[interface{}]interface{})
	Notify(c Context, entry Entity)
	Output(enCh chan Entity)
}