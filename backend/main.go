package main

import (
	"path/filepath"
	"io/ioutil"
	"time"
	"fmt"
	"sort"
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"strings"
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
	TEMPLATE
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
		case "-template":
			t = TEMPLATE
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

func ParseTemplateParams(args []string) (string, []string) {
	resArgs := make([]string, 0)
	templateParams := ""
	for i := 0; i < len(args); i++ {
		if args[i] == "-name" {
			if i+1 == len(args) {
				log.Fatal(errors.New("template name is not specified"))
			}
			templateParams = args[i+1]
			i++
			continue
		}

		resArgs = append(resArgs, args[i])
	}

	return templateParams, resArgs
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
	log.Println(configFile)
	templateParams, args:= ParseTemplateParams(args)
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
	case TEMPLATE:
		log.Println(templateParams)
		w, err := os.Create("templates.tar.gz")
		if err != nil {
			panic(err)
		}
		gw := gzip.NewWriter(w)
		tw := tar.NewWriter(gw)

		uniques := make([]string, 1)
		uniquesWithoutChains := make([]string, 1)
		slice := strings.Split(templateParams, "_")
		if len(slice) != 2 {
			panic("template param error")
		}
		uniques = append(uniques, templateParams)
		uniquesWithoutChains = append(uniquesWithoutChains, slice[0])
		uniques = unique(uniques)
		uniquesWithoutChains = unique(uniquesWithoutChains)
		sort.Strings(uniques)
		sort.Strings(uniquesWithoutChains)

		a3mOffset := 0
		a3mData := strings.Builder{}
		a3mIndex := strings.Builder{}
		a3m := Reader[string]{}
		base := config.Paths.ColabFold.Pdb70 + "_a3m"
		err = a3m.Make(base+".ffdata", base+".ffindex")
		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < len(uniques); i++ {
			a3mid, ok := a3m.Id(uniques[i])
			if ok == false {
				continue
			}

			entry := a3m.Data(a3mid)
			entryLen := len(entry) + 1
			a3mData.WriteString(entry)
			a3mData.WriteRune(rune(0))
			a3mIndex.WriteString(fmt.Sprintf("%s\t%d\t%d\n", uniques[i], a3mOffset, entryLen))
			a3mOffset += entryLen
		}

		now := time.Now()
		if err := AddTarEntry(tw, "pdb70_a3m.ffdata", a3mData.String(), now); err != nil {
			panic(err)
		}
		if err := AddTarEntry(tw, "pdb70_a3m.ffindex", a3mIndex.String(), now); err != nil {
			panic(err)
		}

		for i := 0; i < len(uniquesWithoutChains); i++ {
			pdbacc := strings.ToLower(uniquesWithoutChains[i])
			if len(pdbacc) < 4 {
				fmt.Errorf("Invalid PDB accession %s", pdbacc)
			}

			pdbmid := pdbacc[1:3]
			pdbdivided := config.Paths.ColabFold.PdbDivided
			pdbobsolete := config.Paths.ColabFold.PdbObsolete

			file, err := os.Open(filepath.Join(pdbdivided, pdbmid, pdbacc+".cif.gz"))
			if errors.Is(err, os.ErrNotExist) {
				file, err = os.Open(filepath.Join(pdbobsolete, pdbmid, pdbacc+".cif.gz"))
				if err != nil {
					panic(err)
				}
			} else if err != nil {
				panic(err)
			}

			reader, err := gzip.NewReader(file)
			if err != nil {
				panic(err)
			}

			cif, err := ioutil.ReadAll(reader)
			if err != nil {
				panic(err)
			}

			if err := AddTarEntry(tw, pdbacc+".cif", string(cif), now); err != nil {
				panic(err)
			}

			if err := reader.Close(); err != nil {
				panic(err)
			}

			if err := file.Close(); err != nil {
				panic(err)
			}
		}

		if err := tw.Close(); err != nil {
			panic(err)
		}

		if err := gw.Close(); err != nil {
			panic(err)
		}
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
		if len(mode)>4 && mode[:4] == "pair" {
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
