package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"runtime"
	"github.com/jeffail/tunny"
	"github.com/astaxie/flatmap"
	"github.com/go-steem/rpc"
	"github.com/go-steem/rpc/transports/websocket"
	r "gopkg.in/dancannon/gorethink.v2"
)

func main() {
	if err := run(); err != nil {
		fmt.Println("Error:", err)
	}
}

// Set the settings for the DB
func run() (err error) {
	Rsession, err := r.Connect(r.ConnectOpts{
		Addresses: []string{"138.201.198.167:28015", "138.201.198.169:28015", "138.201.198.173:28015", "138.201.198.175:28015"},
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	// Create a table in the DB
	var rethinkdbname = "steemit75"
	_, err = r.DBCreate(rethinkdbname).RunWrite(Rsession)
	Rsession.Use(rethinkdbname)
	if err != nil {
		fmt.Println("rethindb DB already made")
	}

	_, err = r.DB(rethinkdbname).TableCreate("transactions").RunWrite(Rsession)
	if err != nil {
		fmt.Println("Probably already made a table for transactions")

	}

	_, err = r.DB(rethinkdbname).TableCreate("flatblocks").RunWrite(Rsession)
	if err != nil {
		fmt.Println("Probably already made a table for flat blocks")

	}

	_, err = r.DB(rethinkdbname).TableCreate("operations").RunWrite(Rsession)
	if err != nil {
		fmt.Println("Probably already made a table for flat blocks")

	}

	// Process flags.
	flagAddress := flag.String("rpc_endpoint", "ws://192.168.194.91:8090", "steemd RPC endpoint address")
	flagReconnect := flag.Bool("reconnect", true, "enable auto-reconnect mode")
	flag.Parse()

	var (
		url       = *flagAddress
		reconnect = *flagReconnect
	)

	// Start catching signals.
	var interrupted bool
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// Drop the error in case it is a request being interrupted.
	defer func() {
		if err == websocket.ErrClosing && interrupted {
			err = nil
		}
	}()
	// This allows you to tell the app which block to start on.
	// TODO: Make all of the vars into a config file and package the binaries

	// Start the connection monitor.
	monitorChan := make(chan interface{}, 1)
	if reconnect {
		go func() {
			for {
				event, ok := <-monitorChan
				if ok {
					log.Println(event)
				}
			}
		}()
	}

	// Instantiate the WebSocket transport.
	log.Printf("---> Dial(\"%v\")\n", url)
	t, err := websocket.NewTransport(url,
		websocket.SetAutoReconnectEnabled(reconnect),
		websocket.SetAutoReconnectMaxDelay(30*time.Second),
		websocket.SetMonitor(monitorChan))
	if err != nil {
		return err
	}

	// Use the transport to get an RPC client.
	client, err := rpc.NewClient(t)
	if err != nil {
		return err
	}
	defer func() {
		if !interrupted {
			client.Close()
		}
	}()

	// Start processing signals.
	go func() {
		<-signalCh
		fmt.Println()
		log.Println("Signal received, exiting...")
		signal.Stop(signalCh)
		interrupted = true
		client.Close()
	}()

numCPUs := runtime.NumCPU()
pool, _ := tunny.CreatePoolGeneric(numCPUs).Open()
prop := client.Database.GetDynamicGlobalProperties()
lb := prop.LastIrreversibleBlockNum
queue := make(chan int, 3)
for U := 1; U < lb; U++ {
queue <- U
}
// This is where the work actually happens
	for elem U < lb := range queue {
		pool.SendWork(func(queue chan, lb int, Rsession *r.Session, client *rpc.Client))
	blocknum := uint32(queue)
	 // blockchain reads
	 block, err := client.Database.GetBlock(blocknum)
	 blockraw, err := client.Database.GetBlockRaw(blocknum)
	 var transdata = blockraw
	 var mp map[string]interface{}
	 if err := json.Unmarshal([]byte(*transdata), &mp); err != nil {
		 log.Fatal(err)
	 }
	 Fm, err := flatmap.Flatten(mp)
	 // Rethinkdb writes
	 r.Table("transactions").
		 Insert(block.Transactions).
		 Exec(Rsession)
	 r.Table("flatblocks").
		 Insert(Fm).
		 Exec(Rsession)
	 r.Table("nestedblocks").
		 Insert(block).
		 Exec(Rsession)
		 blockChannel <- blocknum
			if outputStr, ok := data.(string); ok {
					return ("custom job done: " + outputStr)
	 }

    }
	}
