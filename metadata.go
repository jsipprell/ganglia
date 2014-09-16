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
  getr() (<-chan *Metadata)
  sendr(msg *Metadata)
}

type response struct {
  t reqtype
  r chan *Metadata
}

func (r *response) which() (reqtype) {
  return r.t
}

func (r *response) getr() (<-chan *Metadata) {
  return r.r
}

func (r *response) sendr(msg *Metadata) {
  select {
  case r.r <- msg:
  default:
    close(r.r)
  }
}

type regReq struct {
  response
  metadata *Metadata
}

type getReq struct {
  response
  metric MetricType
}

func makeReq(m MetricType) (*getReq) {
  nr := &getReq{
    response:response{r: make(chan *Metadata,1),
                      t: requestMetadata},
    metric:m,
  }
  return nr
}

func makeReg(m *Metadata) (*regReq) {
  nr := &regReq{
    response:response{r: make(chan *Metadata,1),
                      t: registerMetadata},
    metadata:m,
  }
  return nr
}

// An encapsulated server structure which can cache metadata fed to it by
// ganglia metric id and later return copies of that metadata in respose
// to lookup requests. It is normally not necessary to create GMetadataServer
// objects as simple calling the singleton MetadataServer's Register or
// Lookup methods will auto-start it.
type GMetadataServer struct {
  req chan requestType

  starter sync.Once
}

var (
  MetadataServer GMetadataServer
  MetadataNotFound = errors.New("Ganglia metadata not found")
  uidMap = make(map[int] uid)
  uidMutex sync.Mutex
)

func init() {
  uidMap[0] = uid(0x43ff1)
}

func uidGenerator(seed int) uid {
  uidMutex.Lock()
  defer uidMutex.Unlock()

  last,ok := uidMap[seed]
  if !ok {
    last = uid(seed)^0x42
  }
  last++
  uidMap[seed] = last
  if last == uid(0) {
    log.Fatalf("bad uid 0")
  }
  return last
}

func (s *GMetadataServer) start() {
  var metadata []Metadata = make([]Metadata,0)
  var registry map[uid] *Metadata = make(map[uid] *Metadata)
  var idmap map[string] uid = make(map[string] uid)
  var wg sync.WaitGroup
  wg.Add(1)
  defer wg.Wait()

  go func() {
    var mid *MetricIdentifier
    var meta *Metadata
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
          panic("Attempt to register metadata without a metric id")
        } else if m.metric_id.id != uid(0) {
          meta,ok = registry[m.metric_id.id]
          if !ok {
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
              //log.Printf("REGISTER: %v/%v: %v", name, id, m.Type)
              nr.sendr(m)
              continue
            }
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
          //log.Printf("REGISTER: %v/%v: %v", name, id, m.Type)
          nr.sendr(m)
        }
      }
    }
  }()
}

// Register new metadata with the metadata server so that it will auto-associate
// with any metrics seen in the future. Registing pre-existing metadata is a no-op
// but will not produce an error.
func (s *GMetadataServer) Register(md *Metadata) (rmd *Metadata, err error) {
  s.starter.Do(s.start)

  r := makeReg(md)
  s.req <- r
  rmd = <-r.getr()

  if rmd == nil || rmd.metric_id == nil || rmd.metric_id.id == uid(0) {
    err = MetadataNotFound
    rmd = nil
  }
  return
}

// Lookup any metadata previously registered for a an object that can respond
// validly to a MetricId() call. If no metadata is found, MetadataNotFound
// is returned as an error.
func (s *GMetadataServer) Lookup(obj MetricType) (md *Metadata, err error) {
  s.starter.Do(s.start)

  r := makeReq(obj)
  s.req <- r
  md = <-r.getr()
  if md == nil {
    err = MetadataNotFound
  }
  return
}
