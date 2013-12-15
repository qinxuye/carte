package carte

import (
	"bufio"
	"io"
	"os"
	"strings"
	"strconv"
	"fmt"
)

const (
	HG_FILE_ILLEGAL = "Hypergraph file not illegal"
	DATA_FILE_ILLEGAL = "Data init file not illegal"
)

type Env struct {
	edgeMap map[uint64]*HEdge
	vertexMap map[uint64]*Vertex
	cfg *Config
}

func NewEnv(cfg *Config) *Env {
	edgeMap := make(map[uint64]*HEdge)
	vertexMap := make(map[uint64]*Vertex)
	return &Env{edgeMap, vertexMap, cfg}
}

func (env *Env) parseLine(str string) {
	splits := strings.Split(str, " ")
	vsize := len(splits) - 1
	if vsize == 0 {
		return
	}
	
	eidint, er := strconv.Atoi(splits[0])
	if er != nil {
		panic("")
	}
	eid := uint64(eidint)
	edge := NewHEdge(eid)
	
	vids := make([]uint64, vsize)
	hostIdxes := make([]int, vsize)
	locateLocal := false
	
	for idx, vdata := range(splits[1:]) {
		ss := strings.Split(vdata, "/")
		if len(ss) != 2 {
			panic(HG_FILE_ILLEGAL)
		}
		vidint, err := strconv.Atoi(ss[0])
		if err != nil {
			panic(HG_FILE_ILLEGAL)
		}
		vids[idx] = uint64(vidint)
		
		hostIdx, err := strconv.Atoi(ss[1])
		if err != nil {
			panic(HG_FILE_ILLEGAL)
		}
		hostIdxes[idx] = hostIdx
		if hostIdx == env.cfg.currentIdx {
			locateLocal = true
		}
	} 
	
	if !locateLocal {
		return
	}
	
	env.edgeMap[eid] = edge
	
	masterHostIdx := hostIdxes[int(eid) % vsize]
	isLocalMaster := masterHostIdx == env.cfg.currentIdx
	edge.isMaster = isLocalMaster
	edge.degree = vsize
	edge.location = env.cfg.currentIdx
	
	remoteInfos := make(map[int]bool)
	for _, hostIdx := range(hostIdxes) {
		if hostIdx == env.cfg.currentIdx {
			continue
		}
		if _, ok := remoteInfos[hostIdx]; !ok {
			info := HEdgeRemoteInfo {hostIdx == masterHostIdx, hostIdx}
			edge.remoteInfos = append(edge.remoteInfos, info)
			remoteInfos[hostIdx] = true
		}
	}
	edge.isCut = len(edge.remoteInfos) > 0
	
	for idx, vid := range(vids) {
		edge.AddVertexInfo(&VertexInfo{vid, hostIdxes[idx]})
		if hostIdxes[idx] == env.cfg.currentIdx {
			vertex, ok := env.vertexMap[vid]
			if !ok {
				vertex = NewVertex(vid)
				env.vertexMap[vid] = vertex
			}
			edgeInfo := &HEdgeInfo{eid, masterHostIdx, vsize}
			vertex.AddHEdge(edgeInfo)
			
			edge.AddVertex(vertex)
		}
	}
}

func (env *Env) LoadFromFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		panic("hypergraph file does not exist")
		return
	}
	
	defer file.Close()
	
	br := bufio.NewReader(file)
	for {
		line, isPrefix, err1 := br.ReadLine()
		if err1 != nil {
			if err1 != io.EOF {
				panic("Cannot read hypergraph file")
			}
			break
		}
		if isPrefix {
			panic("A too long line unexpected, will use local config")
			return
		}
		
		str := strings.TrimSpace(string(line))
		if len(str) == 0 {
			continue
		}
		
		env.parseLine(str)
	}
	
	
}

func (env *Env) LoadInitFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("hypergraph file does not exist\n")
		return
	}
	
	defer file.Close()
	
	br := bufio.NewReader(file)
	for {
		line, isPrefix, err1 := br.ReadLine()
		if err1 != nil {
			if err1 != io.EOF {
				panic("Cannot read hypergraph file")
			}
			break
		}
		if isPrefix {
			panic("A too long line unexpected, will use local config")
			return
		}
		
		str := strings.TrimSpace(string(line))
		if len(str) == 0 {
			continue
		}
		
		splits := strings.Split(str, " ")
		
		tid := splits[0]
		var obj Entity
		if strings.HasPrefix(tid, "v") {
			vidint, err := strconv.Atoi(tid[1:])
			if err != nil {
				panic(DATA_FILE_ILLEGAL)
			}
			vid := uint64(vidint)
			vobj, ok := env.vertexMap[vid]
			if !ok {
				continue
			}
			obj = vobj
		} else if strings.HasPrefix(tid, "e") {
			eidint, err := strconv.Atoi(tid[1:])
			if err != nil {
				panic(DATA_FILE_ILLEGAL)
			}
			eid := uint64(eidint)
			eobj, ok := env.edgeMap[eid]
			if !ok {
				return
			}
			obj = eobj
		}
		
		if obj == nil {
			panic(DATA_FILE_ILLEGAL)
		}
		
		var key string
		var value interface{}
		for idx, split := range(splits[1:]) {
			if idx % 2 == 0 {
				key = split
			} else {
				fVal, err := strconv.ParseFloat(split, 64)
				if err != nil {
					value = split
				} else {
					value = fVal
				}
				
				obj.SetProperty(key, value)
			}
		}
		
	}
}

