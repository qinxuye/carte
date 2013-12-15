package carte

import (

)

type ContextImpl struct {
	edge *HEdge
	sch *Scheduler
}

func NewContextImpl(edge *HEdge, sch *Scheduler) *ContextImpl {
	return &ContextImpl{edge, sch}
}

func (c *ContextImpl) GetContextObj() ContextObj {
	return c.edge
}

func (c *ContextImpl) CollectLocalNbors(vertex Entity) []Entity {
	vertices := make([]Entity, 0)
	for _, v := range(c.edge.vertices) {
		if v.GetId() != vertex.GetId() {
			vertices = append(vertices, v)
		}
	}
	return vertices
}

func (c *ContextImpl) CollectNborIDs(vertex Entity) []uint64 {
	ids := make([]uint64, 0)
	for _, vi := range(c.edge.vertexInfos) {
		if vi.GetId() != vertex.GetId() {
			ids = append(ids, vi.GetId())
		}
	}
	return ids
}

func (c *ContextImpl) Activate(edgeInfo HEdgeInfo) {
	c.sch.Activate(edgeInfo)
}
