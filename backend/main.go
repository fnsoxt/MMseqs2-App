package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type RunType int

const (
	LOCAL RunType = iota
	WORKER
	SERVER
	CLI
)

func ParseType(args []string) (RunType, []string) {
	resArgs := make([]string, 0)
	t := SERVER
	for _, arg := range args {
		switch arg {
		case "-worker":
			t = WORKER
			continue
		case "-server":
			t = SERVER
			continue
		case "-local":
			t = LOCAL
			continue
		case "-cli":
			t = CLI
			continue
		}

		resArgs = append(resArgs, arg)
	}

	return t, resArgs
}

func ParseConfigName(args []string) (string, []string) {
	resArgs := make([]string, 0)
	file := ""
	for i := 0; i < len(args); i++ {
		if args[i] == "-config" {
			if i+1 == len(args) {
				log.Fatal(errors.New("config file name is not specified"))
			}
			file = args[i+1]
			i++
			continue
		}

		resArgs = append(resArgs, args[i])
	}

	return file, resArgs
}

func ParseRequest(args []string) (string, []string) {
	resArgs := make([]string, 0)
	request := ""
	for i := 0; i < len(args); i++ {
		if args[i] == "-request" {
			if i+1 == len(args) {
				log.Fatal(errors.New("request is not specified"))
			}
			request = args[i+1]
			i++
			continue
		}

		resArgs = append(resArgs, args[i])
	}

	return request, resArgs
}

func main() {
	t, args := ParseType(os.Args[1:])
	configFile, args := ParseConfigName(args)
	req, args := ParseRequest(args)

	var config ConfigRoot
	var err error
	if len(configFile) > 0 {
		if _, err := os.Stat(configFile); errors.Is(err, os.ErrNotExist) {
			log.Println("Creating config file: " + configFile)
			err = WriteDefaultConfig(configFile)
			if err != nil {
				panic(err)
			}
		}
		config, err = ReadConfigFromFile(configFile)

	} else {
		config, err = DefaultConfig()
	}
	if err != nil {
		panic(err)
	}

	err = config.ReadParameters(args)
	if err != nil {
		panic(err)
	}

	if err := config.CheckPaths(); err != nil {
		panic(err)
	}

	switch t {
	case WORKER:
		worker(MakeRedisJobSystem(config.Redis), config)
		break
	case SERVER:
		server(MakeRedisJobSystem(config.Redis), config)
		break
	case CLI:
		jobsystem, err := MakeLocalJobSystem(config.Paths.Results)
		if err != nil {
			panic(err)
		}
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigs
			os.Exit(0)
		}()

		// 生成ticket
		type Req struct {
			Query string   `json:"q"`
			Dbs   []string `json:"dbs"`
			Mode  string   `json:"mode"`
			Email string   `json:"email"`
		}

		var jobrequest JobRequest

		var query string
		var dbs []string
		var mode string
		var email string

		var reqMap Req
		err = json.Unmarshal([]byte(req), &reqMap)
		if err != nil {
			log.Println(err)
		}
		query = reqMap.Query
		dbs = reqMap.Dbs
		mode = reqMap.Mode
		email = reqMap.Email

		databases, err := Databases(config.Paths.Databases, true)
		if err != nil {
			log.Fatal(err.Error())
		}
		// log.Println(reqMap)
		if mode[:3] == "pair" {
			jobrequest, err = NewPairJobRequest(query, mode, email)
			if err != nil {
				log.Fatal(err.Error())
			}
		} else {
			jobrequest, err = NewMsaJobRequest(query, dbs, databases, mode, config.Paths.Results, email)
			if err != nil {
				log.Fatal(err.Error())
			}
		}
		// log.Println(jobrequest.Id)
		result, err := jobsystem.NewJob(jobrequest, config.Paths.Results, false)
		log.Println(result)
		if err != nil {
			log.Fatal(err.Error())
		}
		cli(jobrequest, &jobsystem, config)
		// 执行程序
		break
	case LOCAL:
		jobsystem, err := MakeLocalJobSystem(config.Paths.Results)
		if err != nil {
			panic(err)
		}

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigs
			os.Exit(0)
		}()

		loop := make(chan bool)
		for i := 0; i < config.Local.Workers; i++ {
			go worker(&jobsystem, config)
		}
		go server(&jobsystem, config)
		<-loop
		break
	}
}
