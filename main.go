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
  "gopkg.in/go-playground/pool.v3"
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
	flagAddress := flag.String("rpc_endpoint", "ws://138.201.198.169:8090", "steemd RPC endpoint address")
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

	// Keep processing incoming blocks forever.
	fmt.Println("---> Entering the block processing loop")
	for {
		props, err := client.Database.GetDynamicGlobalProperties()
		lb := int(props.LastIrreversibleBlockNum)
		p := pool.NewLimited(100)
		defer p.Close()
		batch := p.Batch()
		go func() {
			for U := 1; lb < U; U++ {
				batch.Queue(readsandWrites(U, Rsession, client))
				fmt.Println(U)
			}
			batch.QueueComplete()
		}()
		if err != nil {
			return err
		}
	}
}

func readsandWrites(U int, Rsession *r.Session, client *rpc.Client) pool.WorkFunc {
	return func(wu pool.WorkUnit) (interface{}, error) {
		u := uint32(U)
		// blockchain reads
		block, err := client.Database.GetBlock(u)
		blockraw, err := client.Database.GetBlockRaw(u)
		var data = blockraw
		var mp map[string]interface{}
		if err := json.Unmarshal([]byte(*data), &mp); err != nil {
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
			fmt.Println(u)
			if err != nil {
				log.Fatal(err)
			}
			return true, nil
		}
}
