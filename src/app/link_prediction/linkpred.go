package main

import (
	"strconv"
	"math"
	"flag"
	"fmt"
	"os"

	"carte"
)

const (
	ALPHA = 0.98
	EPSILON = 0.01
	
	DEFAULT_OUTPUT_NAME = "output.txt"
)

type LinkPredProgram struct {
	outputFile string
}

func (p *LinkPredProgram) NeedInit() bool {
	return true
}

func (p *LinkPredProgram) Map(c carte.Context, v carte.Entity) {
	e := c.GetContextObj()
	if v.HasProperty("rank") {
		rank := v.GetProperty("rank").(float64)
		for _, vid := range(c.CollectNborIDs(v)) {
			val := rank / (float64(e.Degree()) * float64(e.GetDegree(v)))
			e.Emit(vid, val)
		}
	}
}

func (p *LinkPredProgram) Reduce(c carte.Context, values []interface{}) interface{} {
	var result float64 = 0
	for _, val := range(values) {
		result += val.(float64)
	}
	return result
}

func (p *LinkPredProgram) Apply(c carte.Context, entry carte.Entity, m map[interface{}]interface{}) {
	e := c.GetContextObj()
	if entry.TypeName() == "HEdge" {
		if !entry.HasProperty("init") {
			entry.SetProperty("init", true)
			entry.SetProperty("firstinit", true)
		} else if entry.HasProperty("firstinit") {
			entry.RemoveProperty("firstinit")
		}
	} else {
		if !e.HasProperty("init") && entry.HasProperty("rank") {
			return
		}
		
		erank, ok := m[entry.GetId()]
		if ok {
			key := "erank" + strconv.Itoa(int(e.GetId()))
			entry.SetProperty(key, erank)
		} else {
			return
		}
		
		erankKeys := entry.GetPropertyKeysWithPrefix("erank")
		if len(erankKeys) < c.GetContextObj().GetDegree(entry) {
			return
		}
		
		var acc float64 = 0
		for _, key := range(erankKeys) {
			acc += entry.GetProperty(key).(float64)
		}
		
		if !e.HasProperty("init") {
			entry.SetProperty("rank", acc)
		} else {
			entry.SetProperty("oldrank", entry.GetProperty("rank"))
			rank := (1 - ALPHA) + ALPHA * acc
			entry.SetProperty("rank", rank)
		}
	}
}

func (p *LinkPredProgram) Notify(c carte.Context, entry carte.Entity) {
	if entry.TypeName() == "HEdge" {
		if entry.HasProperty("firstinit") {
			c.Activate(*c.GetContextObj().GetInfo())
		}
	} else {
		if !entry.HasProperty("oldrank") || !entry.HasProperty("rank") {
			return
		}
	
		span := math.Abs(entry.GetProperty("rank").(float64) - entry.GetProperty("oldrank").(float64))
		if span > EPSILON {
			infos := entry.(*carte.Vertex).GetHEdgeInfos()
			for _, info := range(infos) {
//				if info.GetId() != c.GetContextObj().GetId() {
				c.Activate(*info)
//				}
			}
		}
	}
}

func (p *LinkPredProgram) SetOutputFile(fileName string) {
	p.outputFile = fileName
}

func (p *LinkPredProgram) Output(enCh chan carte.Entity) {
	fileName := p.outputFile
	if fileName == "" {
		fileName = DEFAULT_OUTPUT_NAME
	}
	
	file, err := os.Create(fileName)
	if err != nil {
		panic("Cannot output result")
	}
	defer file.Close()
	
	for {
		v := <-enCh
		if v == nil {
			break
		}
		if v.TypeName() == "Vertex" {
			if v.HasProperty("t") && v.GetProperty("t") == "u" {
				file.WriteString(fmt.Sprintf("%d: %.3f\n", int(v.GetId()), v.GetProperty("rank")))
			}
		}
	}
}

var configFile *string = flag.String("c", "config.txt", "File of Config, include instance list")
var hgFile *string = flag.String("h", "hg.txt", "HyperGraph data file")
var initDataFile *string = flag.String("d", "init.txt", "Data init file")
var hostIndex *int = flag.Int("i", 0, "host index of instance list")
var outputFile *string = flag.String("o", DEFAULT_OUTPUT_NAME, "Output file")

func main() {
	flag.Parse()
	
	fmt.Printf("Start to run link prediction\n")
	
	program := new(LinkPredProgram)
	program.SetOutputFile(*outputFile)
	runtime := carte.NewRuntime(program)
	runtime.Init(*hostIndex, *hgFile, *initDataFile, *configFile)
	runtime.Start()
}