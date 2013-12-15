package carte

import (
	"container/list"
	"sync"
)

type Queue struct {
	lock *sync.Mutex
	list *list.List
	containMap map[interface{}]bool
}

func NewQueue() *Queue {
	lock := new(sync.Mutex)
	list := list.New()
	containMap := make(map[interface{}]bool)
	return &Queue{lock, list, containMap}
}

func(q *Queue) Size() int {
	return q.list.Len()
}

func (q *Queue) Enqueue(val interface{}) *list.Element {
	q.lock.Lock()
	defer q.lock.Unlock()
	
	_, ok := q.containMap[val]
	if !ok {
		e := q.list.PushFront(val)
		q.containMap[val] = true
		return e
	}
	return nil
}

func(q *Queue) Dequeue() *list.Element {
	q.lock.Lock()
	defer q.lock.Unlock()
	
	e := q.list.Back()
	if e != nil {
		q.list.Remove(e)
		delete(q.containMap, e.Value)
	}
	return e
}

func(q *Queue) Contain(val interface{}) bool {
	_, ok := q.containMap[val]
	return ok
}