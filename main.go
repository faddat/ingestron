package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"runtime"

	"github.com/jeffail/tunny"
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
pool, err := tunny.CreatePoolGeneric(numCPUs).Open()
if err != nil {
	return err
}
prop, err := client.Database.GetDynamicGlobalProperties()
if err != nil {
	return err
}
lb := uint32(prop.LastIrreversibleBlockNum)
U := uint32(1)

// This is where the work actually happens
		for U <= lb {
		pool.SendWork(func(U uint32, Rsession *r.Session, client *rpc.Client) error {
	 // blockchain reads
	 block, err := client.Database.GetBlock(U)
	 // Rethinkdb writes
	 r.Table("transactions").
		 Insert(block.Transactions).
		 Exec(Rsession)
	 r.Table("nestedblocks").
		 Insert(block).
		 Exec(Rsession)
		 U++
		 	return err
})
	 }
	 return err
	 }
