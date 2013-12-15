package carte

import (
	"sync"
	"strings"
)

type Entry struct {
	id uint64
	properties map[interface{}]interface{}
	lock *sync.RWMutex
}

type VertexInfo struct {
	id uint64
	location int
}

type Vertex struct {
	Entry
	edgeInfos []*HEdgeInfo
	l *sync.Mutex
}

type HEdgeInfo struct {
	id uint64
	location int
	degree int
}

type HEdge struct {
	Entry
	degree int
	location int
	vertices []*Vertex
	vertexInfos []*VertexInfo
	isCut bool
	isMaster bool
	remoteInfos []HEdgeRemoteInfo
	
	mrKV map[interface{}][]interface{}
}

type HEdgeRemoteInfo struct {
	isMaster bool
	location int
}

func (entry Entry) GetId() uint64 {
	return entry.id
}

func (entry *Entry) GetProperty(key interface{}) interface{} {
	entry.lock.RLock()
	defer entry.lock.RUnlock()
	obj, _ := entry.properties[key]
	return obj
}

func (entry *Entry) SetProperty(key interface{}, value interface{}) {
	entry.lock.Lock()
	defer entry.lock.Unlock()
	entry.properties[key] = value
}

func (entry *Entry) HasProperty(key interface{}) bool {
	entry.lock.RLock()
	defer entry.lock.RUnlock()
	_, ok := entry.properties[key]
	return ok
}

func (entry *Entry) RemoveProperty(key interface{}) {
	if _, ok := entry.properties[key]; ok {
		delete(entry.properties, key)
	}
}

func (entry *Entry) GetPropertyKeys() []interface{} {
	keys := []interface{} {}
	for k, _ := range(entry.properties) {
		keys = append(keys, k)
	}
	return keys
}

func (entry *Entry) GetPropertyKeysWithPrefix(prefix string) []interface{} {
	keys := []interface{} {}
	for k, _ := range(entry.properties) {
		sKey, ok := k.(string)
		if ok {
			if strings.HasPrefix(sKey, prefix) {
				keys = append(keys, k)
			}
		}
	}
	return keys
}

func (vi *VertexInfo) GetId() uint64 {
	return vi.id
}

func NewVertex(id uint64) *Vertex {
	properties := make(map[interface{}]interface{})
	lock := new(sync.RWMutex)
	edges := make([]*HEdgeInfo, 0)
	return &Vertex{Entry{id, properties, lock}, edges, new(sync.Mutex)}
}

func (v *Vertex) AddHEdge(ei *HEdgeInfo) {
	v.edgeInfos = append(v.edgeInfos, ei)
}

func (v *Vertex) GetHEdgeInfos() []*HEdgeInfo {
	return v.edgeInfos
}

func (v *Vertex) TypeName() string {
	return "Vertex"
}

func (ei *HEdgeInfo) GetId() uint64 {
	return ei.id
}

func NewHEdge(id uint64) *HEdge {
	properties := make(map[interface{}]interface{})
	lock := new(sync.RWMutex)
	vertices := make([]*Vertex, 0)
	vertexInfos := make([]*VertexInfo, 0)
	remoteInfos := make([]HEdgeRemoteInfo, 0)
	mrKV := make(map[interface{}][]interface{})
	return &HEdge{Entry{id, properties, lock}, 
						0, 0, vertices, vertexInfos, 
						false, false, remoteInfos, mrKV} 
}

func (e *HEdge) AddVertex(v *Vertex) {
	e.vertices = append(e.vertices, v)
}

func (e *HEdge) AddVertexInfo(vi *VertexInfo) {
	e.vertexInfos = append(e.vertexInfos, vi)
}

func (e *HEdge) Emit(k interface{}, v interface{}) {
	existVal, ok := e.mrKV[k]
	if !ok {
		existVal = make([]interface{}, 0)
		e.mrKV[k] = existVal
	}
	e.mrKV[k] = append(existVal, v)
}

func (e *HEdge) ClearMR() {
	e.mrKV = make(map[interface{}][]interface{})
}

func (e *HEdge) GetInfo() *HEdgeInfo {
	return &HEdgeInfo{e.id, e.location, e.degree}
}

func (e *HEdge) Degree() int {
	return len(e.vertexInfos)
}

func (e *HEdge) GetDegree(vertex Entity) int {
	return len(vertex.(*Vertex).edgeInfos)
}

func (e *HEdge) TypeName() string {
	return "HEdge"
}