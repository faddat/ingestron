package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/asdine/storm"
	"github.com/baabeetaa/rpc"
	"github.com/baabeetaa/rpc/transports/websocket"
	"github.com/baabeetaa/rpc/types"
	"github.com/cstockton/go-conv"
)

func main() {
	if err := run(); err != nil {
		log.Fatalln("Error:", err)
	}
}

func run() (err error) {
	// Process flags.
	flagAddress := flag.String("rpc_endpoint", "ws://127.0.0.1:8090", "steemd RPC endpoint address")
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

	// Get config.
	log.Println("---> GetConfig()")
	config, err := client.Database.GetConfig()
	if err != nil {
		return err
	}
	//Open Storm
	db, err := storm.Open("my.db", storm.Batch())
	defer db.Close()
	//start at block 1
	StartBlock := uint32(1)

	// Keep processing incoming blocks forever.
	log.Printf("---> Entering the block processing loop (last block = %v)\n", StartBlock)
	for {
		// Get current properties.
		props, err := client.Database.GetDynamicGlobalProperties()
		if err != nil {
			return err
		}
		lastBlock := props.LastIrreversibleBlockNum

		// Process new blocks.
		for StartBlock < lastBlock {
			block, err := client.Database.GetBlock(StartBlock)
			if err != nil {
				return err
			}
			fmt.Println(conv.Uint(StartBlock))

			// Process the transactions.
			for _, tx := range block.Transactions {
				for _, operation := range tx.Operations {
					switch op := operation.Data().(type) {
					//process votes
					case *types.VoteOperation:
						fmt.Printf("@%v voted for @%v/%v\n", op.Voter, op.Author, op.Permlink)
						vote := op
						err := db.Save(&vote)
						if err != nil {
							return err
						}
					//process account creations
					case *types.AccountCreateOperation:
						accountCreate := op
						fmt.Printf("@%v created @v/%v/%v/%v/%v/%v/%v/%v\n", accountCreate.Creator, accountCreate.Active, accountCreate.Fee, accountCreate.JsonMetadata, accountCreate.MemoKey, accountCreate.NewAccountName, accountCreate.Owner, accountCreate.Posting)
						err := db.Save(&accountCreate)
						if err != nil {
							return err
						}
					//process witness votes
					case *types.AccountWitnessVoteOperation:
						witnessVote := op
						fmt.Printf(witnessVote.Account, witnessVote.Approve, witnessVote.Witness)
						err := db.Save(&witnessVote)
						if err != nil {
							return err
						}
					//process account updates
					case *types.AccountUpdateOperation:
						accountUpdate := op
						err := db.Save(&accountUpdate)
						if err != nil {
							return err
						}
					//process posts and comments
					case *types.CommentOperation:
						commentOperation := op
						err := db.Save(&commentOperation)
						if err != nil {
							return err
						}
					//process follows
					case *types.FollowOperation:
						followOperation := op
						err := db.Save(&followOperation)
						if err != nil {
							return err
						}
					case *types.CommentOptionsOperation:
						commentOptions := op
						err := db.Save(&commentOptions)
						if err != nil {
							return err
						}
					case *types.WithdrawVestingOperation:
						withdrawVesting := op
						err := db.Save(&withdrawVesting)
						if err != nil {
							return err
						}
					}
				}
			}
			StartBlock++
		}

		// Sleep for STEEMIT_BLOCK_INTERVAL seconds before the next iteration.
		time.Sleep(time.Duration(config.SteemitBlockInterval) * time.Second)
	}
}
