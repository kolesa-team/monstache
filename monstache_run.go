package main

import (
	"strings"
	"sync"
	"fmt"
	"strconv"
	"./worker"
	"log"
	"os"
)

var errorLog = log.New(os.Stderr, "ERROR ", log.Flags())

var wg[256] sync.WaitGroup
var shardCount int
var shardUrls[256] string
var neverEndingLock sync.WaitGroup
var stopChannel[256] chan struct{}

func main() {
	var i int

	for {
		neverEndingLock.Add(1)

		config := worker.InitializeConfig()

		if config.UseMongos {
			mongo, err := config.DialMongo()
			if err != nil {
				errorLog.Panicf("Unable to connect to mongodb using URL %s: %s", config.MongoURL, err)
			}

			col := mongo.DB("config").C("shards")
			var shards []map[string]string
			col.Find(nil).All(&shards)

			i = 0

			for _, shard := range shards {
				url := strings.Split(shard["host"], "/")[1]
				replicaName := strings.Split(shard["host"], "/")[0]

				shardUrls[i] = url + "?replicaSet=" + replicaName
				i++
			}

			shardCount = i
		} else {
			shardCount   = 1
			shardUrls[0] = ""
		}

		for i = 0; i < shardCount; i++ {
			wg[i] = sync.WaitGroup{}
			go channelRunner(i, shardUrls[i], config)
		}

		neverEndingLock.Wait()
	}
}

func channelRunner(channelNumber int, rewriteMongoUrl string, config worker.ConfigOptions) {
	fmt.Println("Connecting to shard #" + strconv.Itoa(channelNumber) + " with mongo url: " + rewriteMongoUrl)
	for {
		wg[channelNumber].Add(1)
		worker.Job(&wg[channelNumber], stopChannel[channelNumber], rewriteMongoUrl, config, channelNumber)
		fmt.Println("Restarting shard #" + strconv.Itoa(channelNumber) + " with mongo url: " + rewriteMongoUrl)
		wg[channelNumber].Wait()
	}
}
