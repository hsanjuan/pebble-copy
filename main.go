package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ipfs-cluster/ipfs-cluster/config"
	"github.com/ipfs-cluster/ipfs-cluster/datastore/pebble"

	ipfscluster "github.com/ipfs-cluster/ipfs-cluster"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
)

func main() {
	logging.SetLogLevel("pebble", "DEBUG")

	cfg1 := &pebble.Config{}
	cfgMgr1 := config.NewManager()
	defer cfgMgr1.Shutdown()
	cfgMgr1.RegisterComponent(config.Cluster, &ipfscluster.Config{})
	cfgMgr1.RegisterComponent(config.Datastore, cfg1)

	cfg2 := &pebble.Config{}
	cfgMgr2 := config.NewManager()
	defer cfgMgr2.Shutdown()
	cfgMgr2.RegisterComponent(config.Cluster, &ipfscluster.Config{})
	cfgMgr2.RegisterComponent(config.Datastore, cfg2)

	if len(os.Args) > 1 {
		cfgMgr1.Default()
		cfg1.Folder = "pebble1"
		err := cfgMgr1.SaveJSON("config1.json")

		if err != nil {
			panic(err)
		}
		cfgMgr2.Default()
		cfg2.Folder = "pebble2"
		err = cfgMgr2.SaveJSON("config2.json")
		if err != nil {
			panic(err)
		}
		return
	}

	err := cfgMgr1.LoadJSONFromFile("config1.json")
	if err != nil {
		panic(err)
	}
	err = cfgMgr1.Validate()
	if err != nil {
		panic(err)
	}

	err = cfgMgr2.LoadJSONFromFile("config2.json")
	if err != nil {
		panic(err)
	}
	err = cfgMgr2.Validate()
	if err != nil {
		panic(err)
	}

	peb1, err := pebble.New(cfg1)
	if err != nil {
		panic(err)
	}
	defer peb1.Close()
	peb2, err := pebble.New(cfg2)
	if err != nil {
		panic(err)
	}
	defer peb2.Close()

	ctx := context.Background()

	batchingDs := peb2.(datastore.Batching)
	b, err := batchingDs.Batch(ctx)
	if err != nil {
		panic(err)
	}
	defer b.Commit(ctx)

	results, err := peb1.Query(ctx, query.Query{})
	if err != nil {
		panic(err)
	}
	defer results.Close()

	size := 0
	totalSize := 0
	for r := range results.Next() {
		if r.Error != nil {
			panic(r.Error)
		}

		err = b.Put(ctx, datastore.NewKey(r.Key), r.Value)
		if err != nil {
			panic(err)
		}

		size += r.Size
		if size > (10 << 20) {
			totalSize += size
			if (totalSize/1024/1024/1024)%10 == 0 { // 10GiBs
				fmt.Println("Size written:", totalSize)
			}
			size = 0
			err = b.Commit(ctx)
			if err != nil {
				panic(err)
			}
		}
	}
	fmt.Println("finished")
}
