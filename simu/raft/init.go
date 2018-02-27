package raft

import (
	"math/rand"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

func init() {
	if err := os.MkdirAll("./log/", 0777); err != nil {
		panic(err)
	}

	file, err := os.Create("./log/info")
	if err != nil {
		panic(err)
	}

	// Log as JSON instead of the default ASCII formatter.
	// log.SetFormatter(&log.JSONFormatter{})

	log.SetOutput(file)
	log.SetLevel(log.DebugLevel)

	// set random seed.
	rand.Seed(time.Now().Unix())
}
