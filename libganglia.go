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
type Slope uint
const (
  SLOPE_ZERO          Slope = C.GANGLIA_SLOPE_ZERO
  SLOPE_POSITIVE      Slope = C.GANGLIA_SLOPE_POSITIVE
  SLOPE_NEGATIVE      Slope = C.GANGLIA_SLOPE_NEGATIVE
  SLOPE_BOTH          Slope = C.GANGLIA_SLOPE_BOTH
  SLOPE_UNSPECIFIED   Slope = C.GANGLIA_SLOPE_UNSPECIFIED
  SLOPE_DERIVATIVE    Slope = C.GANGLIA_SLOPE_DERIVATIVE
)

// The maximum possible size of a ganglia packet per libganglia.
const GANGLIA_MAX_MESSAGE_LEN int = C.GANGLIA_MAX_MESSAGE_LEN

// All the ganglia packet types as of Ganglia 3.7.0
type MsgFormat uint16
const (
  GMETADATA_FULL             MsgFormat = C.gmetadata_full
  GMETRIC_USHORT             MsgFormat = C.gmetric_ushort
  GMETRIC_SHORT              MsgFormat = C.gmetric_short
  GMETRIC_INT                MsgFormat = C.gmetric_int
  GMETRIC_UINT               MsgFormat = C.gmetric_uint
  GMETRIC_STRING             MsgFormat = C.gmetric_string
  GMETRIC_FLOAT              MsgFormat = C.gmetric_float
  GMETRIC_DOUBLE             MsgFormat = C.gmetric_double
  GMETADATA_REQUEST          MsgFormat = C.gmetadata_request
)

// All the ganglia value types supported as of Ganglia 3.7.0
type ValueType int8
const (
  VALUE_UNKNOWN         ValueType = C.GANGLIA_VALUE_UNKNOWN
  VALUE_STRING          ValueType = C.GANGLIA_VALUE_STRING
  VALUE_UNSIGNED_SHORT  ValueType = C.GANGLIA_VALUE_UNSIGNED_SHORT
  VALUE_SHORT           ValueType = C.GANGLIA_VALUE_SHORT
  VALUE_UNSIGNED_INT    ValueType = C.GANGLIA_VALUE_UNSIGNED_INT
  VALUE_INT             ValueType = C.GANGLIA_VALUE_INT
  VALUE_FLOAT           ValueType = C.GANGLIA_VALUE_FLOAT
  VALUE_DOUBLE          ValueType = C.GANGLIA_VALUE_DOUBLE
)

// Represents go access to libganglia/apr memory pools. Not required
// for simple xdr ganglia metric decoding.
type Pool struct {
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
  releasePool chan *Pool
  rootPool *Pool
}

var (
  valueTypeMap map[ValueType] reflect.Type
  GangliaValueTypeError = errors.New("Unknown ganglia value type")
)

func init() {
  valueTypeMap = make(map[ValueType] reflect.Type)

  valueTypeMap[VALUE_UNKNOWN] = reflect.TypeOf(nil)
  valueTypeMap[VALUE_STRING] = reflect.TypeOf(string(""))
  valueTypeMap[VALUE_UNSIGNED_SHORT] = reflect.TypeOf(uint16(0))
  valueTypeMap[VALUE_SHORT] = reflect.TypeOf(int16(0))
  valueTypeMap[VALUE_UNSIGNED_INT] = reflect.TypeOf(uint32(0))
  valueTypeMap[VALUE_INT] = reflect.TypeOf(int32(0))
  valueTypeMap[VALUE_FLOAT] = reflect.TypeOf(float32(0))
  valueTypeMap[VALUE_DOUBLE] = reflect.TypeOf(float64(0))
}

//export helper_debug
func helper_debug(msg string) {
  log.Printf(msg)
}

func (vt ValueType) String() string {
  T,ok := valueTypeMap[vt]
  if !ok {
    return "ganglia_value_invalid"
  }
  return "ganglia_value_" + T.String()
}

func getGangliaValueKind(t ValueType) (kind reflect.Kind, err error) {
  typ,ok := valueTypeMap[t]
  if ok {
    kind = typ.Kind()
  } else {
    err = GangliaValueTypeError
  }
  return
}

func getGangliaValueType(t ValueType) (T reflect.Type, err error) {
  typ,ok := valueTypeMap[t]
  if ok {
    T = typ
  } else {
    err = GangliaValueTypeError
  }
  return
}

// convert any value to the appropriate ganglia value type and return a generic
// Value or error if the conversion isn't possible.
func NewValue(t ValueType, v interface{}) (V reflect.Value, err error) {
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
func ConstValue(t ValueType, v interface{}) reflect.Value {
  V,err := NewValue(t,v)
  if err != nil {
    panic(fmt.Sprintf("cannot create constant: %v",err))
  }
  return V
}

var (
  mainPoolManager *poolManager
  mainPool *Pool
  startup sync.Once
)

func (pool Pool) Lock() {
  //log.Printf("%v LOCK",pool)
  pool.mutex.Lock()
}

func (pool Pool) Unlock() {
  //log.Printf("%v UNLOCK",pool)
  pool.mutex.Unlock()
}

// Runs the pool manager
func (p *poolManager) run(started chan struct{}) {
  var count uint
  var pool Pool
  var wg sync.WaitGroup

  p.reqPool = make(chan poolRequest,1)
  p.releasePool = make(chan *Pool,5)
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
      go func(pool *Pool, p *C.struct_Ganglia_pool) {
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
    mainPool = &Pool{
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
func CreatePool() (pool *Pool, err error) {
  pool = &Pool{
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
