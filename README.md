# Ingestron Steem -> Rethinkdb Ingestor in Golang

#### BETA!  

You can build some of the earlier commits.  I pushed the current master (October 6 2016) to demonstrate some code changes.  I am aware it does not compile.  

Sipmly put, Ingestron eats blocks and shits rows.

Ingestron is a golang-based content and informatics ingestor for graphene-based blockchains.  

Its development is funded by Justin Fondreist of BeyondBitcoin to advance the ecosystems of Bitshares, STEEM, Golos, and PeerPlays.  If you wish to see features added to
INGESTRON, please contact Jacob Gadikian on Google Hangouts at faddat@gmail.com. 

https://docs.google.com/drawings/d/1iV7vcO0CZCYbiRWAjKN41axR6ZaeZ-OFD9YA39jFdoI/edit?usp=sharing

Ingestron will serve an integrated status-panel WEB UI at port 6969, because why not?  Status information on all daemons
monitored by INGESTRON is written to RethinkDB with each block.  We'll be working to integrate tightly with the blockchain
itself in coming releases, hopefully going as far as using cgo for actual integration.

INGESTRON is a little like an old-school computer game:

### Get Started by:  (I'm moving the development work to a devel branch and putting up an old ingestron for now.  Please know that there is much more ingestron to come.  
go get github.com/faddat/ingestron


### Environmental Expectations:
* Rethinkdb at 127.0.0.1:28015
* Bitshares at 127.0.0.1:8091
* Steemd at 127.0.0.1::8090



