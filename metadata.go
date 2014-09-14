package ganglia

import (
  "errors"
  "sync"
  "log"
)

type uid uint64

type reqtype uint8
const (
  requestMetadata reqtype = iota
  registerMetadata
)

type requestType interface {
  which() (reqtype)
  getr() (<-chan *GangliaMetadata)
  sendr(msg *GangliaMetadata)
}

type response struct {
  t reqtype
  r chan *GangliaMetadata
}

func (r *response) which() (reqtype) {
  return r.t
}

func (r *response) getr() (<-chan *GangliaMetadata) {
  return r.r
}

func (r *response) sendr(msg *GangliaMetadata) {
  select {
  case r.r <- msg:
  default:
    close(r.r)
  }
}

type regReq struct {
  response
  metadata *GangliaMetadata
}

type getReq struct {
  response
  metric GangliaMetricType
}

func makeReq(m GangliaMetricType) (*getReq) {
  nr := &getReq{
    response:response{r: make(chan *GangliaMetadata,1),
                      t: requestMetadata},
    metric:m,
  }
  return nr
}

func makeReg(m *GangliaMetadata) (*regReq) {
  nr := &regReq{
    response:response{r: make(chan *GangliaMetadata,1),
                      t: registerMetadata},
    metadata:m,
  }
  return nr
}

type metadataServer struct {
  req chan requestType

  starter sync.Once
}

var (
  GangliaMetadataServer metadataServer
  GangliaMetadataNotFound error
  uidMap map[int] uid
  uidMutex sync.Mutex
)

func init() {
  GangliaMetadataNotFound = errors.New("Ganglia metadata not found")
  uidMap = make(map[int] uid)
  uidMap[0] = uid(0x43ff1)
}

func uidGenerator(seed int) uid {
  uidMutex.Lock()
  defer uidMutex.Unlock()

  last,ok := uidMap[seed]
  if !ok {
    last = uid(seed)^0x43ff
  }
  last++
  uidMap[seed] = last
  if last == uid(0) {
    log.Fatalf("bad uid 0")
  }
  return last
}

func (s *metadataServer) start() {
  var metadata []GangliaMetadata = make([]GangliaMetadata,0)
  var registry map[uid] *GangliaMetadata = make(map[uid] *GangliaMetadata)
  var idmap map[string] uid = make(map[string] uid)
  var wg sync.WaitGroup
  wg.Add(1)
  defer wg.Wait()

  go func() {
    var mid *GangliaMetricId
    var meta *GangliaMetadata
    var ok bool

    defer func() {
      err := recover()
      if err != nil {
        log.Fatalf("METADATA SERVER PANIC: %v", err)
      }
    }()

    req := make(chan requestType, 32)
    s.req = req
    defer close(s.req)
    wg.Done()

    for {
      nr := <-req
      switch(nr.which()) {
      case requestMetadata:
        m := nr.(*getReq).metric
        mid = m.MetricId()
        if mid != nil {
          if mid.id == uid(0) {
            name := mid.Host + "/" +mid.Name
            id,ok := idmap[name]
            if !ok {
              id = uidGenerator(0)
              idmap[name] = id
            }
            mid.id = id
          }
          meta,ok = registry[mid.id]
          if ok {
            nr.sendr(meta.copy())
            continue
          }
        }
        close(nr.(*getReq).r)
      case registerMetadata:
        m := nr.(*regReq).metadata
        if m.metric_id == nil {
          log.Fatalf("Attempt to register metadata without a metric id")
        } else if m.metric_id.id != uid(0) {
          meta,ok = registry[m.metric_id.id]
          if !ok {
            log.Printf("MAP: %v",idmap)
            log.Fatalf("Internal inconsistency, metric id (%v) not registered",m.metric_id.id)
          }
          nr.sendr(meta)
          continue
        }
        name := m.metric_id.Host + "/" +m.metric_id.Name
        id,ok := idmap[name]
        if ok {
          meta,ok := registry[id]
          if !ok {
            if m.metric_id.id == uid(0) {
              m.metric_id.id = id
              l := len(metadata)
              metadata = append(metadata,*m)
              m = &(metadata[l])
              registry[id] = m
              log.Printf("REGISTER: %v/%v: %v", name, id, m.Type)
              nr.sendr(m)
              continue
            }
            log.Printf("MAP: %v",idmap)
            log.Printf("META: %v",metadata)
            log.Fatalf("Internal inconsistency, metric id (%v/%v) not registered",id,m.metric_id.id)
          }
          m.metric_id = meta.metric_id
          nr.sendr(m)
        } else {
          id = uidGenerator(0)
          idmap[name] = id
          m.metric_id.id = id
          l := len(metadata)
          metadata = append(metadata,*m)
          m = &(metadata[l])
          registry[id] = m
          log.Printf("REGISTER: %v/%v: %v", name, id, m.Type)
          nr.sendr(m)
        }
      }
    }
  }()
}

func (s *metadataServer) Register(md *GangliaMetadata) (rmd *GangliaMetadata, err error) {
  s.starter.Do(s.start)

  r := makeReg(md)
  s.req <- r
  rmd = <-r.getr()

  if rmd == nil || rmd.metric_id == nil || rmd.metric_id.id == uid(0) {
    err = GangliaMetadataNotFound
    rmd = nil
  }
  return
}

func (s *metadataServer) Lookup(obj GangliaMetricType) (md *GangliaMetadata, err error) {
  s.starter.Do(s.start)

  r := makeReq(obj)
  s.req <- r
  md = <-r.getr()
  if md == nil {
    err = GangliaMetadataNotFound
  }
  return
}
