package ganglia

/*
#include "helper.h"
*/
import "C"

import (
  "time"
  "errors"
  "sync"
  "unsafe"
  "log"
  "bytes"
)

var (
  shutdown chan struct{}
  wg *sync.WaitGroup
  AlreadyShutdownError = errors.New("Socket reader already shutdown")
  XDRDecodeFailure = errors.New("XDR decode failure")
)

// the bool_t type from xdr is a pain
func xdrBool(v interface{}) (r bool) {
  var i int
  b,ok := v.(int)
  if !ok {
    b,ok := v.(C.bool_t)
    if !ok {
      panic("cannot convert boolean value")
    }
    i = int(C.helper_bool(unsafe.Pointer(&b)))
  } else {
    i = b
  }
  if i != 0 {
    r = true
  }
  return
}

// Performs the actual xdr decode via some C helper functions and libganglia.
func xdrDecode(lock sync.Locker, buf []byte) (msg Message, nbytes int, err error) {
  var xdr *C.XDR
  var cbuf *C.char

  lock.Lock()
  defer lock.Unlock()

  xdr = (*C.XDR)(C.malloc(C.XDR_size))
  defer C.free(unsafe.Pointer(xdr))
  buflen := len(buf)
  if buflen > GANGLIA_MAX_MESSAGE_LEN {
    buflen = GANGLIA_MAX_MESSAGE_LEN
  } else if buflen == 0 {
    panic("empty buffer")
  }

  cbuf = (*C.char)(C.calloc(1,C.size_t(GANGLIA_MAX_MESSAGE_LEN)))
  if cbuf == nil {
    panic("out of memory calling C.calloc")
  }
  defer C.free(unsafe.Pointer(cbuf))
  if buflen > 0 {
    C.memcpy(unsafe.Pointer(cbuf),unsafe.Pointer(&buf[0]),C.size_t(buflen))
  }

  C.xdrmem_create(xdr, cbuf, C.u_int(GANGLIA_MAX_MESSAGE_LEN), C.XDR_DECODE)
  defer C.helper_destroy_xdr(xdr)

  if (cbuf != nil) {
    // perform the actual decode
    var fmsg *C.Ganglia_metadata_msg
    var vmsg *C.Ganglia_value_msg
    var mf *C.Ganglia_msg_formats

    fmsg = (*C.Ganglia_metadata_msg)(C.malloc(C.Ganglia_metadata_msg_size))
    if fmsg == nil {
      panic("out of memory allocating for decoding ganglia xdr msg")
    }
    vmsg = (*C.Ganglia_value_msg)(C.malloc(C.Ganglia_metadata_val_size))
    if vmsg == nil {
      panic("out of memory allocating for decoding ganglia xdr value")
    }
    defer C.free(unsafe.Pointer(fmsg))
    defer C.free(unsafe.Pointer(vmsg))

    mf = (*C.Ganglia_msg_formats)(C.calloc(1,C.size_t(unsafe.Sizeof(*mf))))
    if mf == nil {
      panic("out of memory allocating for ganglia msg formats")
    }
    defer C.free(unsafe.Pointer(mf))
    if !xdrBool(C.helper_init_xdr(xdr,mf)) {
      err = XDRDecodeFailure
      return
    }
    defer C.helper_uninit_xdr(xdr,mf)
    nbytes = int(C.helper_perform_xdr(xdr,fmsg,vmsg,mf))
    if nbytes > 0 {
      var info *MetricInfo
      var metric_id *C.Ganglia_metric_id

      id := MsgFormat(*mf)
      // log.Printf("XDR bytes=%v id=%v", nbytes,id)
      switch(id) {
      case GMETADATA_REQUEST:
        greq := C.Ganglia_metadata_msg_u_grequest(fmsg)
        msg = &MetadataReq{
            gangliaMsg:gangliaMsg{formatIdentifier:id},
            MetricIdentifier:&MetricIdentifier{
                Host:C.GoString(greq.metric_id.host),
                Name:C.GoString(greq.metric_id.name),
                Spoof:xdrBool(greq.metric_id.spoof),
                Exists:true,
            },
         }
         C.helper_free_xdr(xdr,mf,unsafe.Pointer(fmsg))
      case GMETADATA_FULL:
        gfull := C.Ganglia_metadata_msg_u_gfull(fmsg)
        mid := &MetricIdentifier{
                Host:C.GoString(gfull.metric_id.host),
                Name:C.GoString(gfull.metric_id.name),
                Spoof:xdrBool(gfull.metric_id.spoof),
                Exists:true,
            }
        msg = &MetadataDef{
            gangliaMsg:gangliaMsg{formatIdentifier:id},
            MetricIdentifier:mid,
            metric:Metadata{
                Type:C.GoString(gfull.metric._type),
                Name:C.GoString(gfull.metric.name),
                Units:C.GoString(gfull.metric.units),
                Tmax:uint(gfull.metric.tmax),
                Dmax:uint(gfull.metric.dmax),
                metric_id:mid,
            },
          }
        //log.Printf("DEBUG: metadata name=%v/%v type=%v",mid.Name,msg.MetricId().Name,
        //            msg.GetMetadata().Type)
        C.helper_free_xdr(xdr,mf,unsafe.Pointer(fmsg))
      case GMETRIC_STRING:
        gstr := C.Ganglia_value_msg_u_gstr(vmsg)
        metric_id = &gstr.metric_id
        info = &MetricInfo{
          Value: C.GoString(gstr.str),
          Format: C.GoString(gstr.fmt),
        }
      case GMETRIC_USHORT:
        gus := C.Ganglia_value_msg_u_gu_short(vmsg)
        metric_id = &gus.metric_id
        f := C.GoString(gus.fmt)
        if f == "%u" || f == "%hu" {
          f = ""
        }
        info = &MetricInfo{
          Value: uint16(gus.us),
          Format: f,
        }
      case GMETRIC_SHORT:
        gss := C.Ganglia_value_msg_u_gs_short(vmsg)
        metric_id = &gss.metric_id
        f := C.GoString(gss.fmt)
        if f == "%d" || f == "%h" {
          f = ""
        }
        info = &MetricInfo{
          Value: int16(gss.ss),
          Format: f,
        }
      case GMETRIC_UINT:
        gint := C.Ganglia_value_msg_u_gu_int(vmsg)
        metric_id = &gint.metric_id
        f := C.GoString(gint.fmt)
        if f == "%u" {
          f = ""
        }
        info = &MetricInfo{
          Value: uint32(gint.ui),
          Format: f,
        }
      case GMETRIC_INT:
        gint := C.Ganglia_value_msg_u_gs_int(vmsg)
        metric_id = &gint.metric_id
        f := C.GoString(gint.fmt)
        if f == "%d" {
          f = ""
        }
        info = &MetricInfo{
          Value: int32(gint.si),
          Format: f,
        }
        fallthrough
      case GMETRIC_FLOAT:
        gflt := C.Ganglia_value_msg_u_gf(vmsg)
        metric_id = &gflt.metric_id
        info = &MetricInfo{
          Value: float32(gflt.f),
          Format: C.GoString(gflt.fmt),
        }
      case GMETRIC_DOUBLE:
        gdbl := C.Ganglia_value_msg_u_gd(vmsg)
        metric_id = &gdbl.metric_id
        info = &MetricInfo{
          Value: float64(gdbl.d),
          Format: C.GoString(gdbl.fmt),
        }
      default:
        log.Printf("XDR value decode failure, unsupported metric %v",id)
        C.helper_free_xdr(xdr,mf,unsafe.Pointer(vmsg))
      }
      if err == nil && info != nil {
        if metric_id != nil {
          info.Spoof = xdrBool(metric_id.spoof)
          if metric_id.host != nil {
            info.Host = []byte(C.GoString(metric_id.host))
          }
          if metric_id.name != nil {
            info.Name = []byte(C.GoString(metric_id.name))
          }
        }
        msg,err = NewMetric(id,*info)
        C.helper_free_xdr(xdr,mf,unsafe.Pointer(vmsg))
      }
    }
  }
  // log.Printf("xdr bytes consumed: %v",nbytes)
  if err == nil && msg != nil && !msg.HasMetadata() {
    md,err := MetadataServer.Lookup(msg)
    if err == nil {
      if md == nil {
        panic("bad metadata from metadata server")
      }
      msg.(*gmetric).metadata = md
      //log.Printf("SET MD for msg %v to %v",msg.(*gmetric).Name,msg.GetMetadata().Type)
    }
  }
  return
}

// Shuts down the currently running xdr decoder.
func ShutdownXDRDecoder() (err error) {
  select {
  case <-shutdown:
    err =  AlreadyShutdownError
  default:
    close(shutdown)
  }
  if err == nil && wg != nil {
    defer func() {
      wg = nil
    }()
    defer wg.Wait()
  }
  return
}

// Starts goroutines to read raw data from a channel, send it to a decoder
// which will instantiate new message objects as sufficient data becomes availeble.
// These messages will be sent to the output channel. If no receiver is available
// for 100ms a panic will occur.
//
// A pre-built buffer may be supplied as the second argument and it will
// be decoded and emptied immediately before any other data. If it contains
// insufficient data to decode it will be placed at the beginning of the receive
// buffer and retried once more data becomes available.
//
// TODO: mulitplexing.
func StartXDRDecoder(input <-chan []byte,
                     inbuf []byte,
                     sout chan Message)  (err error) {

  var outchans []chan Message
  locker := getPoolManager()

  wg = new(sync.WaitGroup)
  shutdown = make(chan struct{},1)

  if sout != nil {
    outchans = append(outchans,sout)
  }

  dist := make(chan Message,1)

  wg.Add(1)
  defer wg.Done()

  go func(inp <-chan Message, outputs []chan Message) {
    wg.Add(1)
    defer wg.Wait()
    defer wg.Done()

    defer func() {
      err := recover()
      if err != nil {
        log.Fatalf("XDR DECODER PANIC: %v", err)
      }
    }()

    for {
      select {
      case <-shutdown:
        for _,c := range outputs {
          select {
          case <-c:
          default:
            close(c)
          }
        }
        return
      case msg := <-inp:
        for _,c := range outputs {
          if c != nil {
            select {
            case <-time.After(time.Duration(100) * time.Millisecond):
              panic("100ms timeout blocking on sending buffer to decoder(s)")
            case c <- msg:
            }
          }
        }
      }
    }
  }(dist,outchans)

  go func(inp <-chan []byte, outp chan Message, sprev []byte) {
    var msg []byte
    inbuf := bytes.NewBuffer(sprev)
    var nbytes int = inbuf.Len()

    _ = nbytes
    wg.Add(1)
    defer wg.Wait()
    defer wg.Done()

    defer func() {
      err := recover()
      if err != nil {
        log.Fatalf("XDR DECODER PANIC: %v",err)
      }
    }()

    for {
      if inbuf.Len() == 0 {
        inbuf.Truncate(0)
      }
      select {
      case msg = <-inp:
        if msg != nil {
          _,err := inbuf.Write(msg)
          if err != nil {
            log.Fatalf("pre-xdr input buffer: %v", err)
          }
        }
      case <-shutdown:
        return
      }

      for l := inbuf.Len(); l > 0; l = inbuf.Len() {
        if l > GANGLIA_MAX_MESSAGE_LEN {
          panic("input buffer exceeded maximum ganglia message length")
        }

        outbuf := inbuf.Bytes()
        msg, nbytes, err := xdrDecode(locker,outbuf)
        // log.Printf("msg=%v, nbytes=%v, err=%v",msg,nbytes,err)
        if err != nil {
          log.Printf("xdr decode error (%v/%v bytes): %v", l, nbytes, err)
          msg = nil
          if nbytes == 0 {
            // skip at least one byte on error if someone is doing
            // something to get us out of sync
            nbytes = 1
          }
        }
        if msg != nil {
          select {
          case _ = <-time.After(time.Duration(500) * time.Millisecond):
              log.Printf("WARNING: dropping message, output blocked for 500ms")
          case outp <- msg:
          }
        }
        inbuf.Next(nbytes)
      }
    }
  }(input,dist,inbuf)
  return
}

// vi: set sts=2 sw=2 ai et tw=0 syntax=go:
