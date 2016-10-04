package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-steem/rpc"
	"github.com/go-steem/rpc/transports/websocket"
	r "gopkg.in/dancannon/gorethink.v2"
)

type Connecty struct {
	session *r.Session
	client  *rpc.Client
}

const (
	numberGoroutines = 8
)

var wg sync.WaitGroup

func main() {
	if err := run(); err != nil {
		log.Fatalln("Error:", err)
	}
}

// Set the settings for the DB
func run() (err error) {

	connected := Connect()
	// Keep processing incoming blocks forever.
	fmt.Println("---> Entering the block processing loop")
	for {
		// Get current properties.
		props, err := connected.client.Database.GetDynamicGlobalProperties()
		taskLoad := props.LastIrreversibleBlockNum
		tasks := make(chan uint32, 1000)
		if err != nil {
			return err
		}
		wg.Add(numberGoroutines)
		for gr := 1; gr <= numberGoroutines; gr++ {
			go Worker(tasks, gr, connected)
		}

		for U := uint32(1); U <= taskLoad; U++ {
			tasks <- U
		}
		return err
	}
}

func Worker(tasks chan uint32, gr int, connected Connecty) {
	defer wg.Done()
	task, ok := <-tasks
	if !ok {
		fmt.Printf("worker: %d : Shutting Down\n", Worker)
	}
	fmt.Println(task)
	fmt.Println(gr)
	block, err := connected.client.Database.GetBlock(task)
	r.Table("transactions").
		Insert(block.Transactions).
		Exec(connected.session)
	fmt.Println(err)

}

func Connect() Connecty {

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
		fmt.Println(err)
	}

	// Use the transport to get an RPC client.
	client, err := rpc.NewClient(t)
	if err != nil {
		fmt.Println(err)
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
	var connected Connecty
	connected.client = client
	connected.session = Rsession
	return connected
}
