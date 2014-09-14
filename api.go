// go side of the api
package main

/*
#cgo CFLAGS: -I/nm/local/include -I/usr/local/include
#cgo LDFLAGS: -L/nm/local/lib64 -L/usr/local/lib64 -lganglia

#include "helper.h"
*/
import "C"

import (
  _"fmt"
  "sync"
  "unsafe"
)

type GangliaPool struct {
  mutex sync.Mutex
  c C.Ganglia_pool
  refcnt uint
  manager *poolManager
  notify chan C.Ganglia_pool
}

type poolRequest struct {
  response chan C.Ganglia_pool
}

type poolManager struct {
  reqPool chan poolRequest
  releasePool chan *GangliaPool
}

var (
  mainPoolManager *poolManager
  mainPool *GangliaPool
  startup sync.Once
)

func (pool GangliaPool) Lock() {
  pool.mutex.Lock()
}

func (pool GangliaPool) Unlock() {
  pool.mutex.Unlock()
}

func (p *poolManager) run(started chan C.Ganglia_pool) {
  var count uint
  var pool GangliaPool
  var wg sync.WaitGroup

  p.reqPool = make(chan poolRequest,1)
  p.releasePool = make(chan *GangliaPool,5)
  wg.Add(1)
  go func(parent C.Ganglia_pool) {
    defer wg.Done()
    pool.mutex.Lock()
    defer pool.mutex.Unlock()
    pool.c = C.Ganglia_pool_create(parent)
  }(pool.c)
  wg.Wait()

  for {
    select {
    case req := <-p.reqPool:
      go func(parent C.Ganglia_pool, req *poolRequest) {
        var newpool C.Ganglia_pool
        defer func(p C.Ganglia_pool) { req.response <- p}(newpool)
        pool.mutex.Lock()
        defer pool.mutex.Unlock()
        count++
        newpool = C.Ganglia_pool_create(parent)
        if started != nil {
          select {
          case <-started:
            started = nil
            break
          default:
            started <- newpool
          }
        }
      }(pool.c,&req)
    case relpool := <-p.releasePool:
      go func(pool *GangliaPool, p C.Ganglia_pool) {
        relpool.mutex.Lock()
        defer relpool.mutex.Unlock()
        relpool.c = nil
        C.Ganglia_pool_destroy(p)
        count--
      }(relpool,relpool.c)
    }
  }
}

func getPoolManager() *poolManager {
  startup.Do(func() {
    mainPoolManager = new(poolManager)
  })
  started := make(chan C.Ganglia_pool,1)
  defer func() {
    mainPool.c = <-started
    close(started)
  }()
  go mainPoolManager.run(started)
  return mainPoolManager
}

func CreatePool() (pool *GangliaPool, err error) {
  pool = &GangliaPool{
    manager: getPoolManager(),
    notify: make(chan C.Ganglia_pool,1),
  }

  wait := make(chan C.Ganglia_pool)
  pool.manager.reqPool <-poolRequest{ response: wait }
  pool.c = <-wait
  return
}

func xdrCall(lock sync.Locker, buf []byte) (msg GangliaMetadataMessage, err error) {
  var cres C.int
  var xdr C.XDR
  var xdr_state C.int = C.int(0)
  var ids []C.Ganglia_msg_formats
  var cbuf *C.char
  var bufs []*C.char
  var id C.Ganglia_msg_formats
  lock.Lock()
  defer lock.Unlock()

  buflen := len(buf)
  if buflen > GANGLIA_MAX_MESSAGE_LEN {
    buflen = GANGLIA_MAX_MESSAGE_LEN
  }

  cbuf = (*C.char)(C.calloc(1,C.size_t(GANGLIA_MAX_MESSAGE_LEN)))
  if cbuf == nil {
    panic("out of memory calling C.calloc")
  }
  defer C.free(unsafe.Pointer(cbuf))
  if len(buf) > 0 {
    C.memcpy(unsafe.Pointer(cbuf),unsafe.Pointer(&buf[0]),C.size_t(buflen))
  }

  C.xdrmem_create(&xdr, cbuf, C.u_int(GANGLIA_MAX_MESSAGE_LEN), C.XDR_DECODE)
  defer func(x *C.XDR, state *C.int) {
    defer C.helper_destroy_xdr(x)
    if int(*state) > 0 {
      for i,id := range ids {
        buf := bufs[i]
        bufs[i] = nil
        res := C.helper_free_xdr(x,&id,unsafe.Pointer(buf))
        *state = C.int(int(*state)-int(res))
      }
    }
  }(&xdr, &xdr_state)

  for {
    // perform the actual decode
    var fmsg C.Ganglia_metadata_msg
    var vmsg C.Ganglia_value_msg

    var p unsafe.Pointer
    var i C.Ganglia_msg_formats
    C.helper_init_xdr(&xdr,&i,C.int(0))
    cres = C.helper_perform_xdr(&xdr,&xdr_state,&fmsg,&vmsg,&i)

    for c := 0; c < int(cres); c++ {
      switch(i) {
      case C.gmetadata_request:
        p = unsafe.Pointer(&fmsg)
        msg = &GangliaMetadataReq{
            gangliaBaseMessage:gangliaBaseMessage{formatIdentifier:GangliaMsgFormat(i)},
            GangliaMetricId:&GangliaMetricId{
                Host:C.GoString(fmsg.Ganglia_metadata_msg_u.gfull[1].metric_id.Host),
                Name:C.GoString(fmsg.Ganglia_metadata_msg_u[1].metric_id.name),
                Spoof:bool(fmsg.Ganglia_metadata_msg_u[1].spoof),
                Active:true,
            },
         }
      case C.gmetadata_full:
        p = unsafe.Pointer(&fmsg)
        msg = &GangliaMetadataDef{
            gangliaBaseMessage:gangliaBaseMessage{formatIdentifier:GangliaMsgFormat(i)},
            GangliaMetricId:&GangliaMetricId{
                Host:C.GoString(fmsg.Ganglia_metadata_msg_u[0].gfull.metric_id.Host),
                Name:C.GoString(fmsg.Ganglia_metadata_msg_u[0].gfull.metric_id.name),
                Spoof:bool(fmsg.Ganglia_metadata_msg_u[0].gfull.spoof),
                Active:true,
            },
            metric:GangliaMetadata{
                Type:C.GoString(fmsg.Ganglia_metadata_msg_u[0].gfull.metric._type),
                Name:C.GoString(fmsg.Ganglia_metadata_msg_u[0].gfull.metric.name),
                Units:C.GoString(fmsg.Ganglia_metadata_msg_u[0].gfull.metric.units),
                Tmax:uint(fmsg.Ganglia_metadata_msg_u[0].gfull.metric.tmax),
                Dmax:uint(fmsg.Ganglia_metadata_msg_u[0].gfull.metric.dmax),
            },
          }
      default:
      }

      if xdr_state > C.int(0) {
        ids = append(ids,i)
        bufs = append(bufs,p)
      }
      break
    }
  }
  return
}


// vi: set syntax=go:
