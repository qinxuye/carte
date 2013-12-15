package carte

import (
	"bufio"
	"io"
	"os"
	"fmt"
	"strings"
	"strconv"
)

const (
	DEFAULT_HOST = "localhost"
	DEFAULT_PORT = 31413
)

type Config struct {
	hosts []string
	ports []int
	currentIdx int
	hostsCount int
}

func NewConfig() *Config {
	return &Config{make([]string, 0), make([]int, 0), 0, 1}
}

func (cfg *Config) LoadFromFile(path string) {
	defaultConfig := func() {
		cfg.hosts = append(cfg.hosts, DEFAULT_HOST)
		cfg.ports = append(cfg.ports, DEFAULT_PORT)
	}

	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("Config file does not exist, will use local config\n")
		defaultConfig()
		return
	}
	
	defer file.Close()
	
	br := bufio.NewReader(file)
	for {
		line, isPrefix, err1 := br.ReadLine()
		if err1 != nil {
			if err1 != io.EOF {
				panic("Cannot read config file")
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
		
		splits := strings.Split(str, ":")
		if len(splits) > 2 {
			panic("Config file illegal")
		} else {
			cfg.hosts = append(cfg.hosts, splits[0])
			if len(splits) == 2 {
				port, er := strconv.Atoi(splits[1])
				if er != nil {
					port = DEFAULT_PORT
				}
				cfg.ports = append(cfg.ports, port) 
			} else {
				cfg.ports = append(cfg.ports, DEFAULT_PORT)
			}
		}
	}
	
	cfg.hostsCount = len(cfg.hosts)
}