package ganglia

/*
#cgo CFLAGS: -I/nm/local/include -I/usr/local/include
#cgo LDFLAGS: -L/nm/local/lib64 -L/usr/local/lib64 -lganglia

#include "helper.h"
*/
import "C"

import (
  "reflect"
  "bytes"
  "log"
  "fmt"
  "sync"
  "unsafe"
  "errors"
)

// Matches ganglia/rrd slope types
type GangliaSlope uint
const (
  GANGLIA_SLOPE_ZERO          GangliaSlope = C.GANGLIA_SLOPE_ZERO
  GANGLIA_SLOPE_POSITIVE      GangliaSlope = C.GANGLIA_SLOPE_POSITIVE
  GANGLIA_SLOPE_NEGATIVE      GangliaSlope = C.GANGLIA_SLOPE_NEGATIVE
  GANGLIA_SLOPE_BOTH          GangliaSlope = C.GANGLIA_SLOPE_BOTH
  GANGLIA_SLOPE_UNSPECIFIED   GangliaSlope = C.GANGLIA_SLOPE_UNSPECIFIED
  GANGLIA_SLOPE_DERIVATIVE    GangliaSlope = C.GANGLIA_SLOPE_DERIVATIVE
)

const GANGLIA_MAX_MESSAGE_LEN int = 1500

// All the ganglia packet types as of Ganglia 3.7.0
type GangliaMsgFormat uint16
const (
  GMETADATA_FULL             GangliaMsgFormat = C.gmetadata_full
  GMETRIC_USHORT             GangliaMsgFormat = C.gmetric_ushort
  GMETRIC_SHORT              GangliaMsgFormat = C.gmetric_short
  GMETRIC_INT                GangliaMsgFormat = C.gmetric_int
  GMETRIC_UINT               GangliaMsgFormat = C.gmetric_uint
  GMETRIC_STRING             GangliaMsgFormat = C.gmetric_string
  GMETRIC_FLOAT              GangliaMsgFormat = C.gmetric_float
  GMETRIC_DOUBLE             GangliaMsgFormat = C.gmetric_double
  GMETADATA_REQUEST          GangliaMsgFormat = C.gmetadata_request
)

// All the ganglia value types supported as of Ganglia 3.7.0
type GangliaValueType int8
const (
  GANGLIA_VALUE_UNKNOWN         GangliaValueType = C.GANGLIA_VALUE_UNKNOWN
  GANGLIA_VALUE_STRING          GangliaValueType = C.GANGLIA_VALUE_STRING
  GANGLIA_VALUE_UNSIGNED_SHORT  GangliaValueType = C.GANGLIA_VALUE_UNSIGNED_SHORT
  GANGLIA_VALUE_SHORT           GangliaValueType = C.GANGLIA_VALUE_SHORT
  GANGLIA_VALUE_UNSIGNED_INT    GangliaValueType = C.GANGLIA_VALUE_UNSIGNED_INT
  GANGLIA_VALUE_INT             GangliaValueType = C.GANGLIA_VALUE_INT
  GANGLIA_VALUE_FLOAT           GangliaValueType = C.GANGLIA_VALUE_FLOAT
  GANGLIA_VALUE_DOUBLE          GangliaValueType = C.GANGLIA_VALUE_DOUBLE
)

// Represents go access to libganglia/apr memory pools. Not required
// for simple xdr ganglia metric decoding.
type GangliaPool struct {
  mutex *sync.Mutex
  c *C.struct_Ganglia_pool
  refcnt uint
  manager *poolManager
  notify chan *C.struct_Ganglia_pool
}

type poolRequest struct {
  response chan *C.struct_Ganglia_pool
}

type poolManager struct {
  reqPool chan poolRequest
  releasePool chan *GangliaPool
  rootPool *GangliaPool
}

var (
  gangliaValueTypeMap map[GangliaValueType] reflect.Type
  GangliaValueTypeError = errors.New("Unknown ganglia value type")
)

func init() {
  gangliaValueTypeMap = make(map[GangliaValueType] reflect.Type)

  gangliaValueTypeMap[GANGLIA_VALUE_UNKNOWN] = reflect.TypeOf(nil)
  gangliaValueTypeMap[GANGLIA_VALUE_STRING] = reflect.TypeOf(string(""))
  gangliaValueTypeMap[GANGLIA_VALUE_UNSIGNED_SHORT] = reflect.TypeOf(uint16(0))
  gangliaValueTypeMap[GANGLIA_VALUE_SHORT] = reflect.TypeOf(int16(0))
  gangliaValueTypeMap[GANGLIA_VALUE_UNSIGNED_INT] = reflect.TypeOf(uint32(0))
  gangliaValueTypeMap[GANGLIA_VALUE_INT] = reflect.TypeOf(int32(0))
  gangliaValueTypeMap[GANGLIA_VALUE_FLOAT] = reflect.TypeOf(float32(0))
  gangliaValueTypeMap[GANGLIA_VALUE_DOUBLE] = reflect.TypeOf(float64(0))
}

//export helper_debug
func helper_debug(msg string) {
  log.Printf(msg)
}

func (vt GangliaValueType) String() string {
  T,ok := gangliaValueTypeMap[vt]
  if !ok {
    return "ganglia_value_invalid"
  }
  return "ganglia_value_" + T.String()
}

func getGangliaValueKind(t GangliaValueType) (kind reflect.Kind, err error) {
  typ,ok := gangliaValueTypeMap[t]
  if ok {
    kind = typ.Kind()
  } else {
    err = GangliaValueTypeError
  }
  return
}

func getGangliaValueType(t GangliaValueType) (T reflect.Type, err error) {
  typ,ok := gangliaValueTypeMap[t]
  if ok {
    T = typ
  } else {
    err = GangliaValueTypeError
  }
  return
}

// convert any value to the appropriate ganglia value type and return a generic
// Value or error if the conversion isn't possible.
func NewGangliaValue(t GangliaValueType, v interface{}) (V reflect.Value, err error) {
  T,err := getGangliaValueType(t)
  if err != nil {
    return
  }
  value := reflect.ValueOf(v)
  if !value.Type().ConvertibleTo(T) {
    err = GangliaValueTypeError
  } else {
    V = value.Convert(T)
  }
  return
}

// Create a new contstant ganglia value from any compatible go type.
// This should ONLY be used on immutable values.
func ConstGangliaValue(t GangliaValueType, v interface{}) reflect.Value {
  V,err := NewGangliaValue(t,v)
  if err != nil {
    panic(fmt.Sprintf("cannot create constant: %v",err))
  }
  return V
}

var (
  mainPoolManager *poolManager
  mainPool *GangliaPool
  startup sync.Once
)

func (pool GangliaPool) Lock() {
  //log.Printf("%v LOCK",pool)
  pool.mutex.Lock()
}

func (pool GangliaPool) Unlock() {
  //log.Printf("%v UNLOCK",pool)
  pool.mutex.Unlock()
}

// Runs the pool manager
func (p *poolManager) run(started chan struct{}) {
  var count uint
  var pool GangliaPool
  var wg sync.WaitGroup

  p.reqPool = make(chan poolRequest,1)
  p.releasePool = make(chan *GangliaPool,5)
  wg.Add(1)
  go func(parent *C.struct_Ganglia_pool) {
    defer wg.Done()
    pool.c = C.Ganglia_pool_create(parent)
    if pool.c == nil {
      panic("null ganglia pool")
    }
  }(p.rootPool.c)
  wg.Wait()

  defer func() {
    err := recover()
    if err != nil {
      log.Fatalf("POOL MANAGER PANIC: %v", err)
    }
  }()

  if p.rootPool.c == nil {
    p.rootPool.c = pool.c
  }

  pool.mutex = p.rootPool.mutex
  if started != nil {
    select {
    case <-started:
      break
    default:
      close(started)
    }
  }

  for {
    select {
    case req := <-p.reqPool:
      go func(parent *C.struct_Ganglia_pool, req *poolRequest) {
        var newpool *C.struct_Ganglia_pool
        defer func(p *C.struct_Ganglia_pool) { req.response <- p}(newpool)
        pool.Lock()
        defer pool.Unlock()
        defer func() {
          err := recover()
          if err != nil {
            log.Fatalf("POOL REQUEST PANIC: %v", err)
          }
        }()
        count++
        newpool = C.Ganglia_pool_create(parent)
      }(pool.c,&req)
    case relpool := <-p.releasePool:
      go func(pool *GangliaPool, p *C.struct_Ganglia_pool) {
        relpool.Lock()
        defer relpool.Unlock()
        defer func() {
          err := recover()
          if err != nil {
            log.Fatalf("POOL RELEASE PANIC: %v", err)
          }
        }()
        relpool.c = nil
        C.Ganglia_pool_destroy(p)
        count--
      }(relpool,relpool.c)
    }
  }
}

func startupPools() (ok bool) {
  startup.Do(func() {
    mainPoolManager = &poolManager{}
    mainPool = &GangliaPool{
      manager: mainPoolManager,
      notify: make(chan *C.struct_Ganglia_pool, 1),
      mutex: new(sync.Mutex),
    }
    mainPoolManager.rootPool = mainPool
    ok = true
  })
  return
}

func getPoolManager() *poolManager {
  if startupPools() {
    started := make(chan struct{})
    defer func() {
      <-started
    }()
    go mainPoolManager.run(started)
  }
  return mainPoolManager
}

// Lock the pool manager for thread safety
func (p *poolManager) Lock() {
  p.rootPool.Lock()
}

// Unlock the pool manager
func(p *poolManager) Unlock() {
  p.rootPool.Unlock()
}

// Creates a new ganglia memory pool. Not required for
// basic xdr decoding.
func CreatePool() (pool *GangliaPool, err error) {
  pool = &GangliaPool{
    manager: getPoolManager(),
    notify: make(chan *C.struct_Ganglia_pool,1),
    mutex: new(sync.Mutex),
  }

  wait := make(chan *C.struct_Ganglia_pool)
  pool.manager.reqPool <-poolRequest{ response: wait }
  pool.c = <-wait
  return
}

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
func xdrCall(lock sync.Locker, buf []byte) (msg GangliaMetadataMessage, nbytes int, err error) {
  var xbytes C.size_t
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
    var ii *C.Ganglia_msg_formats
    fmsg = (*C.Ganglia_metadata_msg)(C.calloc(1,C.Ganglia_metadata_msg_size))
    vmsg = (*C.Ganglia_value_msg)(C.calloc(1,C.Ganglia_metadata_val_size))
    defer C.free(unsafe.Pointer(fmsg))
    defer C.free(unsafe.Pointer(vmsg))

    ii = (*C.Ganglia_msg_formats)(C.calloc(1,C.size_t(unsafe.Sizeof(*ii))))
    defer C.free(unsafe.Pointer(ii))
    if !xdrBool(C.helper_init_xdr(xdr,ii)) {
      log.Printf("nothing to do");
      return
    }
    defer C.helper_uninit_xdr(xdr,ii)
    xbytes = C.helper_perform_xdr(xdr,fmsg,vmsg,ii)
    nbytes = int(xbytes)
    if nbytes > 0 {
      var info *GMetricInfo
      var metric_id *C.Ganglia_metric_id

      id := GangliaMsgFormat(*ii)
      // log.Printf("XDR bytes=%v id=%v", nbytes,id)
      switch(id) {
      case GMETADATA_REQUEST:
        greq := C.Ganglia_metadata_msg_u_grequest(fmsg)
        msg = &GangliaMetadataReq{
            gangliaMsg:gangliaMsg{formatIdentifier:id},
            GangliaMetricId:&GangliaMetricId{
                Host:C.GoString(greq.metric_id.host),
                Name:C.GoString(greq.metric_id.name),
                Spoof:xdrBool(greq.metric_id.spoof),
                Exists:true,
            },
         }
         C.helper_free_xdr(xdr,ii,unsafe.Pointer(fmsg))
      case GMETADATA_FULL:
        gfull := C.Ganglia_metadata_msg_u_gfull(fmsg)
        mid := &GangliaMetricId{
                Host:C.GoString(gfull.metric_id.host),
                Name:C.GoString(gfull.metric_id.name),
                Spoof:xdrBool(gfull.metric_id.spoof),
                Exists:true,
            }
        msg = &GangliaMetadataDef{
            gangliaMsg:gangliaMsg{formatIdentifier:id},
            GangliaMetricId:mid,
            metric:GangliaMetadata{
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
        C.helper_free_xdr(xdr,ii,unsafe.Pointer(fmsg))
      case GMETRIC_STRING:
        if info == nil {
          gstr := C.Ganglia_value_msg_u_gstr(vmsg)
          metric_id = &gstr.metric_id
          info = &GMetricInfo{
            Value: C.GoString(gstr.str),
            Format: C.GoString(gstr.fmt),
          }
        }
        fallthrough
      case GMETRIC_USHORT:
        if info == nil {
          gus := C.Ganglia_value_msg_u_gu_short(vmsg)
          metric_id = &gus.metric_id
          f := C.GoString(gus.fmt)
          if f == "%u" {
            f = ""
          }
          info = &GMetricInfo{
            Value: uint16(gus.us),
            Format: f,
          }
        }
        fallthrough
      case GMETRIC_SHORT:
        if info == nil {
          gss := C.Ganglia_value_msg_u_gs_short(vmsg)
          metric_id = &gss.metric_id
          f := C.GoString(gss.fmt)
          if f == "%d" {
            f = ""
          }
          info = &GMetricInfo{
            Value: int16(gss.ss),
            Format: f,
          }
        }
        fallthrough
      case GMETRIC_UINT:
        if info == nil {
          gint := C.Ganglia_value_msg_u_gu_int(vmsg)
          metric_id = &gint.metric_id
          f := C.GoString(gint.fmt)
          if f == "%u" {
            f = ""
          }
          info = &GMetricInfo{
            Value: uint32(gint.ui),
            Format: f,
          }
        }
        fallthrough
      case GMETRIC_INT:
        if info == nil {
          gint := C.Ganglia_value_msg_u_gs_int(vmsg)
          metric_id = &gint.metric_id
          f := C.GoString(gint.fmt)
          if f == "%d" {
            f = ""
          }
          info = &GMetricInfo{
            Value: int32(gint.si),
            Format: f,
          }
        }
        fallthrough
      case GMETRIC_FLOAT:
        if info == nil {
          gflt := C.Ganglia_value_msg_u_gf(vmsg)
          metric_id = &gflt.metric_id
          info = &GMetricInfo{
            Value: float32(gflt.f),
            Format: C.GoString(gflt.fmt),
          }
        }
        fallthrough
      case GMETRIC_DOUBLE:
        if info == nil {
          gdbl := C.Ganglia_value_msg_u_gd(vmsg)
          metric_id = &gdbl.metric_id
          info = &GMetricInfo{
            Value: float64(gdbl.d),
            Format: C.GoString(gdbl.fmt),
          }
        }
        fallthrough
      default:
        if info != nil {
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
        } else {
          log.Printf("GOT unsupported metric %v",id)
        }
        C.helper_free_xdr(xdr,ii,unsafe.Pointer(vmsg))
        // log.Printf("helper_free_xdr: %v",res)
      }
    }
  }
  // log.Printf("xdr bytes consumed: %v",nbytes)
  if err == nil && msg != nil && !msg.HasMetadata() {
    md,err := GangliaMetadataServer.Lookup(msg)
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

var (
  shutdown chan struct{}
  wg *sync.WaitGroup
  AlreadyShutdownError = errors.New("Socket reader already shutdown")
)

// Shuts down the currently running socket reader.
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

// Starts goroutines to read data from a socket (data must be provied elsewhere)
// and decode that through the ganglia xdr routines. ganglia messages will be
// send to the sout channel. TODO: mulitplexing.
//
// Raw data send to the input channel will be decoded and sent to the output channel.
func StartXDRDecoder(input <-chan []byte,
                     inbuf *bytes.Buffer,
                     sout chan GangliaMetadataMessage)  (err error) {

  var outchans []chan GangliaMetadataMessage
  locker := getPoolManager()

  wg = new(sync.WaitGroup)
  shutdown = make(chan struct{},1)

  if sout != nil {
    outchans = append(outchans,sout)
  }

  wg.Add(1)
  defer wg.Done()

  dist := make(chan GangliaMetadataMessage,1)
  go func(inp <-chan GangliaMetadataMessage, outputs []chan GangliaMetadataMessage) {
    wg.Add(1)
    defer wg.Done()
    defer wg.Wait()

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
        for i,c := range outputs {
          if c != nil {
            select {
            case c <- msg:
            default:
              outputs[i] = nil
            }
          }
        }
      }
    }
  }(dist,outchans)

  if inbuf == nil {
    inbuf = &bytes.Buffer{}
  }
  go func(inp <-chan []byte, outp chan GangliaMetadataMessage, sprev *bytes.Buffer) {
    var msg []byte
    inbuf := bytes.NewBuffer(sprev.Bytes())
    var nbytes int = inbuf.Len()

    _ = nbytes
    wg.Add(1)
    defer wg.Done()
    defer wg.Wait()

    defer func() {
      err := recover()
      if err != nil {
        log.Fatalf("XDR DECODER PANIC: %v",err)
      }
    }()

    for {
      select {
      case msg = <-inp:
        if msg != nil {
          _,err := inbuf.Write(msg)
          if err != nil {
            log.Fatalf("pre-xdr input buffer: %v", err)
          }
        }
        break
      case <-shutdown:
        return
      }
      l := inbuf.Len()
      if l > 0 {
        outbuf := make([]byte,l,l)
        l,_ = inbuf.Read(outbuf)
        if l > 0 {
          _ = locker
          msg, nbytes, err := xdrCall(mainPool,outbuf)
          // log.Printf("msg=%v, nbytes=%v, err=%v",msg,nbytes,err)
          if err != nil {
            log.Fatalf("xdr decode error (%v/%v bytes): %v", l, nbytes, err)
          }
          if msg != nil {
            outp <- msg
          }
        }
      }
    }
  }(input,dist,inbuf)
  return
}

// vi: set syntax=go:
