package steemconnect

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-steem/rpc"
	"github.com/go-steem/rpc/transports/websocket"
)

//SteemConnect handles making a connection to a local instance of steemd.  In the next revision it'll take a string variable for the steemd address.
func SteemConnect() (client *rpc.Client) {
	// Process flags.
	flagAddress := flag.String("rpc_endpoint", "ws://127.0.0.1:8090", "steemd RPC endpoint address")
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
	var err error

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
	client, err = rpc.NewClient(t)
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
	return
}
