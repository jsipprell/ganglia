package ganglia

/*
#cgo pkg-config: ganglia

#include "helper.h"
*/
import "C"

import (
  "reflect"
  "log"
  "fmt"
  "sync"
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

// The maximum possible size of a ganglia packet per libganglia.
const GANGLIA_MAX_MESSAGE_LEN int = C.GANGLIA_MAX_MESSAGE_LEN

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

// vi: set syntax=go:
