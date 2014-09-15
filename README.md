Ganglia Go Package
==================

A small go package that provides basic access to Ganglia network metrics.

Currently this package only permits decoding of Ganglia XDR packets/streams as
the immediate need was for decode only rather than encode. However, I fully
intend to add an encoder in the near future.

This package requires cgo and an installed libganglia (probably at least version
3.4.0).

The package uses pkg-config however ganglia does *not*. To get around this a
``make-pkgconfig.sh`` script has been supplied. To use:

    $ ./make-pkgconfig.sh > ganglia.pc
    # assuming all went well
    $ export PKG_CONFIG_PATH=`pwd`
    $ go install

Contents
--------

This package provides three main components, only one of which is technically
required for basic metric decode functionality:

1. An xdr decoder service started via ``StartXDRDecoder`` and
stopped via ``StopXDRDecoder``.
1. A metadata service which can collect parsed metadata packets from gmond
and auto-associate metrics with metadata (once the metadata has been seen).
This service is started automatically when the first metadata request
or registration is perfomed. See ``GangliaMetadataServer.Lookup`` and
``GangliaMetadataService.Register``.
1. A simple udp network client that can stream received packets to the xdr
decoder. This is provided as more of an example and will likely be replaced
by something application-specific.

Example Usage
-------------

```go
include main

import (
  "github.com/jsipprell/ganglia"
  "time"
  "log"
  "os"
)

func main() {
  // create the main byte stream packet channel
  c := make(chan []byte,1)
  // the message channel to feed us decoded ganglia messages
  msgchan := make(chan ganglia.GangliaMessage)

  go func() {
    defer func() {
      err := recover()
      if err != nil {
        log.Fatalf("PANIC: %v",err)
      }
    }()
    t := time.Tick(5e9)

    for {
      select {
      case now := <-t:
        log.Printf("TICK: %v",now)
      case msg := <-msgchan:
        if msg.IsMetadataDef() {
          md := msg.GetMetadata()
          _,err := ganglia.GangliaMetadataServer.Register(md)
          if err != nil {
            log.Printf("metadata server error on %v: %v",md,err)
          }
        } else {
          log.Println(msg.String())
        }
      }
    }
  }()
  err := ganglia.StartXDRDecoder(c,nil,msgchan)
  if err != nil {
    log.Fatalf("cannot start xdr decoder: %v",err)
  }

  args := os.Args[1:]
  if len(args) < 1 {
    log.Fatalf("must specific network:port")
  }
  var mcast bool
  if len(args) > 1 && args[0] == "mcast" {
    args = args[1:]
    mcast = true
  }
  // This starts the client and will send packets to:
  // c -> xdr decoder -> above goroutine
  ganglia.Client(args[0],mcast,c)
}
```
