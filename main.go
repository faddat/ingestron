package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/astaxie/flatmap"
	"github.com/go-steem/rpc"
	"github.com/go-steem/rpc/transports/websocket"
	r "gopkg.in/dancannon/gorethink.v2"
)

func main() {
	if err := run(); err != nil {
		log.Fatalln("Error:", err)
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
	var rethinkdbname string = "steemit75"
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
	flagAddress := flag.String("rpc_endpoint", "ws://138.201.198.169:8090", "steemd RPC endpoint address")
	flagReconnect := flag.Bool("reconnect", false, "enable auto-reconnect mode")
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
	Startblock := 1
	U := uint32(Startblock)
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

	// Keep processing incoming blocks forever.
	fmt.Println("---> Entering the block processing loop")
	for {
		// Get current properties.
		props, err := client.Database.GetDynamicGlobalProperties()
		if err != nil {
			return err
		}

		// Process new blocks.
		// This now explodes the JSON for each block.  This will flatten the nested arrays in the JSON.  Unsure if this will yeild the right result but it will be better.
		for props.LastIrreversibleBlockNum-U > 0 {
			block, err := client.Database.GetBlock(U)
			blockraw, err := client.Database.GetBlockRaw(U)
			lastblock := props.LastIrreversibleBlockNum
			var data = blockraw
			var mp map[string]interface{}
			if err := json.Unmarshal([]byte(*data), &mp); err != nil {
				log.Fatal(err)
			}
			fm, err := flatmap.Flatten(mp)
			if err != nil {
				log.Fatal(err)
			}
			var ks []string
			for k := range fm {
				ks = append(ks, k)
			}
			sort.Strings(ks)
			for _, k := range ks {
				fmt.Println(k, ":", fm[k])
			}
			fmt.Println(U)
			fmt.Println(block)
			// uncomment the line below for debugging purposes to see exactly what is being written
			r.Table("transactions").
				Insert(block.Transactions).
				Exec(Rsession)
			r.Table("flatblocks").
				Insert(fm).
				Exec(Rsession)
			r.Table("nestedblocks").
				Insert(blockraw).
				Exec(Rsession)
			if err != nil {
				return err
			}

			// Process the transactions.
			if U != lastblock {
				U++
			}

		}

	}
}
