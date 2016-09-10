package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
        r "gopkg.in/dancannon/gorethink.v2"
	"github.com/go-steem/rpc"
	"github.com/go-steem/rpc/transports/websocket"
)

func main() {
	if err := run(); err != nil {
		log.Fatalln("Error:", err)
	}
}



// Set the settings for the DB
func run() (err error) {
    Rsession, err := r.Connect(r.ConnectOpts{
    Addresses: []string{"138.201.198.167:28015","138.201.198.169:28015","138.201.198.173:28015","138.201.198.175:28015"},
    })
    if err != nil {
        log.Fatalln(err.Error())
    }

// Create a table in the DB
var rethinkdbname string = "steemit1"
_, err = r.DBCreate(rethinkdbname).RunWrite(Rsession)
Rsession.Use(rethinkdbname)
	if err != nil {
		fmt.Println("rethindb DB already made")
}
	_, err = r.DB(rethinkdbname).TableCreate("blocks").RunWrite(Rsession)

	if err != nil {
		fmt.Println("Probably already made a table for blocks")

	}


	// Process flags.
	flagAddress := flag.String("rpc_endpoint", "ws://138.201.198.167:8090", "steemd RPC endpoint address")
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
	Lastblock := 1
	u := uint32(Lastblock)
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
	log.Printf("---> Entering the block processing loop (last block = %v)\n")
	for {
		// Get current properties.
		props, err := client.Database.GetDynamicGlobalProperties()
		if err != nil {
			return err
		}

		// Process new blocks.
		for props.LastIrreversibleBlockNum-u > 0 {
			block, err := client.Database.GetBlock(u)
			fmt.Println(block)
			r.Table("blocks").
			Insert(block).
			Exec(Rsession)
			if err != nil {
				return err
			}

			// Process the transactions.

			u++
}
		}

	}
