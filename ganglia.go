// go package to provide mechanism for decode and encode of
// ganglia metrics. Uses cgo and requires libganglia.
//
// Jesse Sipprell <jessesipprell@gmail.com>
//
// Copyright 2014, All Rights Reserved
package ganglia

import (
  "net"
  "sync"
  "strings"
  "errors"
  "reflect"
  "fmt"
  "log"
)

var (
  formatMap map[GangliaMsgFormat] reflect.Kind
  revFormatMap map[reflect.Kind] GangliaMsgFormat
  GMetricTypeError = errors.New("Type does not match gmetric")
)

func init() {
  formatMap = make(map[GangliaMsgFormat] reflect.Kind)
  revFormatMap = make(map[reflect.Kind] GangliaMsgFormat)

  formatMap[GMETRIC_USHORT] = reflect.Uint16
  revFormatMap[reflect.Uint16] = GMETRIC_USHORT
  formatMap[GMETRIC_SHORT] = reflect.Int16
  revFormatMap[reflect.Int16] = GMETRIC_SHORT
  formatMap[GMETRIC_INT] = reflect.Int32
  revFormatMap[reflect.Int32] = GMETRIC_INT
  revFormatMap[reflect.Int] = GMETRIC_INT
  formatMap[GMETRIC_UINT] = reflect.Uint32
  revFormatMap[reflect.Uint32] = GMETRIC_UINT
  revFormatMap[reflect.Uint] = GMETRIC_UINT
  formatMap[GMETRIC_STRING] = reflect.String
  revFormatMap[reflect.String] = GMETRIC_STRING
  formatMap[GMETRIC_FLOAT] = reflect.Float32
  revFormatMap[reflect.Float32] = GMETRIC_FLOAT
  formatMap[GMETRIC_DOUBLE] = reflect.Float64
  revFormatMap[reflect.Float64] = GMETRIC_DOUBLE
}

func valueof(v interface{}) reflect.Value {
  v1 := reflect.ValueOf(v)
  for v2 := reflect.Zero(reflect.TypeOf(v)); v1 != v2; v1 = reflect.Indirect(v2) {
    v2 = v1
  }

  return v1
}

// For use when constructing new metrics, all fields are optional.
type GMetricInfo struct {
  Value interface{}
  Format string
  Host []byte
  Name []byte
  Spoof bool
}

// All mertrics and metadata must respond with an Id
type GangliaMetricType interface {
  MetricId() *GangliaMetricId
}

// All ganglia messages have an associated format identifer
type GangliaMessage interface {
  FormatId() GangliaMsgFormat
}

type GangliaMetadataHandler interface {
  IsRequest() bool
  HasMetadata() bool
  IsMetadataDef() bool
  GetMetadata() *GangliaMetadata
}

type GangliaMetadataMessage interface {
  GangliaMetadataHandler
  FormatId() GangliaMsgFormat
  MetricId() *GangliaMetricId
}

// The basic metric interface.
type GMetric interface {
  GangliaMetadataHandler
  FormatId() GangliaMsgFormat
  MetricId() *GangliaMetricId
  GetValue() reflect.Value
  SetFormat(f []byte)
  String() string
}

// GangliaMetricId identifies a specific message, metric
// or metadata packet.
type GangliaMetricId struct {
  Host, Name string
  Spoof bool
  Exists bool
  id uid
}

// Return the canonical metric id for a message or metadata
// packet.
func (mid *GangliaMetricId) MetricId() *GangliaMetricId {
  return mid
}

type gangliaMsg struct {
  formatIdentifier GangliaMsgFormat
}

// Returns the canonical format id for any ganglia object
func (msg gangliaMsg) FormatId() (GangliaMsgFormat) {
  return msg.formatIdentifier
}

// Returns a printable version of a ganglia message id
func (msg gangliaMsg) String() (string) {
  return msg.formatIdentifier.String()
}

// Returns true if the object is metadata defintiion.
func (msg *gangliaMsg) IsMetadataDef() bool {
  return false
}

func (sl GangliaSlope) String() (s string) {
  switch(sl) {
  case GANGLIA_SLOPE_ZERO:
    s = "zero"
  case GANGLIA_SLOPE_POSITIVE:
    s = "positive"
  case GANGLIA_SLOPE_NEGATIVE:
    s = "negative"
  case GANGLIA_SLOPE_BOTH:
    s = "both"
  case GANGLIA_SLOPE_DERIVATIVE:
    s = "derivative"
  case GANGLIA_SLOPE_UNSPECIFIED:
    s = "unspecified"
  default:
    s = fmt.Sprintf("unsupported_slope_%d",int(sl))
  }
  return
}

func (id GangliaMsgFormat) String() (s string) {
  switch(id) {
  case GMETADATA_FULL:
    s = "gmetadata_full"
  case GMETRIC_USHORT:
    s = "gmetric_ushort"
  case GMETRIC_SHORT:
    s = "gmetric_short"
  case GMETRIC_INT:
    s = "gmetric_int"
  case GMETRIC_UINT:
    s = "gmetric_uint"
  case GMETRIC_STRING:
    s = "gmetric_string"
  case GMETRIC_FLOAT:
    s = "gmetric_float"
  case GMETRIC_DOUBLE:
    s = "gmetric_double"
  case GMETADATA_REQUEST:
    s = "gmetric_request"
  default:
    s = fmt.Sprintf("unsupported_metric_%d", int(id))
  }
  return
}

type KeyValueMetadata interface {
  Key() (string, error)
  ValueFor(string) (string, error)
}

// Basic ganglia metadata.
type GangliaMetadata struct {
  Type string
  Name string
  Units string
  Slope GangliaSlope
  Tmax, Dmax uint

  extra []KeyValueMetadata
  metric_id *GangliaMetricId
}

// Create a unique copy of some piece of metadata.
// Note that the metric id is never copied and shared
// by all copies.
func (md *GangliaMetadata) copy() (*GangliaMetadata) {
  var extra []KeyValueMetadata

  if md.extra != nil {
    extra = make([]KeyValueMetadata,len(md.extra),len(md.extra))
    copy(extra,md.extra)
  }
  return &GangliaMetadata{
    Type:md.Type,
    Name:md.Name,
    Units:md.Units,
    Slope:md.Slope,
    Tmax:md.Tmax,
    Dmax:md.Dmax,
    extra:extra,
    metric_id:md.metric_id,
  }
}

// A metadata defintion update message from or to
// another ganglia agent.
type GangliaMetadataDef struct {
  gangliaMsg
  *GangliaMetricId
  metric GangliaMetadata
}

// Returns the canonical metric id of a metadata update
func (mdef *GangliaMetadataDef) MetricId() (*GangliaMetricId) {
  if mdef.metric.metric_id != nil {
    return mdef.metric.metric_id
  }
  return mdef.GangliaMetricId
}

func (mdef *GangliaMetadataDef) HasMetadata() bool {
  return true
}

func (mdef *GangliaMetadataDef) IsMetadataDef() bool {
  return true
}

func (mdef *GangliaMetadataDef) GetMetadata() *GangliaMetadata {
  return &mdef.metric
}

func (mdef *GangliaMetadataDef) IsRequest() bool {
  return false
}

// Request metadata update form an agent.
type GangliaMetadataReq struct {
  gangliaMsg
  *GangliaMetricId
}

func (mreq *GangliaMetadataReq) IsRequest() bool {
  return true
}

func (mreq *GangliaMetadataReq) HasMetadata() bool {
  return false
}

func (mreq *GangliaMetadataReq) IsMetadataDef() bool {
  return false
}

func (mreq *GangliaMetadataReq) GetMetadata() *GangliaMetadata {
  return nil
}

type gmetric struct {
  gangliaMsg
  GangliaMetricId
  fmt string
  value reflect.Value
  metadata interface{}
}

func (m *gmetric) HasMetadata() (r bool) {
  if m.metadata != nil {
    r = true
  }
  return
}

func (m *gmetric) GetMetadata() (md *GangliaMetadata) {
  if m.metadata != nil {
    md = m.metadata.(*GangliaMetadata)
  }
  return
}

func (m *gmetric) IsRequest() bool {
  return false
}

func (m *gmetric) MetricId() (*GangliaMetricId) {
  return &m.GangliaMetricId
}

func (m *gmetric) SetFormat(f []byte) {
  m.fmt = string(f)
}

type valueTypes interface {
  Elem() reflect.Value
  CanSet() bool
  CanInterface() bool
  CanAddr() bool
}

func (m *gmetric) setvalue(v interface{}) (err error) {
  var t reflect.Type
  var V reflect.Value
  V,ok := v.(reflect.Value)
  if !ok {
    V = reflect.ValueOf(v)
  }

  t = V.Type()
  id,ok := revFormatMap[t.Kind()]
  if !ok || id != m.FormatId() {
    if !ok {
      err = fmt.Errorf("Type of %v (%v) does not match kind %v",v,t,t.Kind())
    } else {
      err = fmt.Errorf("Ganglia type identifier %v does not match %v/%v",
                       m.FormatId(),id,t.Kind())
    }
    return
  }

  if !m.value.IsValid() {
    m.value = V
  } else {
    m.value.Set(V)
  }
  if m.fmt == "" {
    m.fmt = "%v"
  }
  return
}

func (m *gmetric) IsKind(kinds ...reflect.Kind) (ok bool) {
  kind := m.value.Kind()
  for _,k := range kinds {
    if k == kind {
      ok = true
      break
    }
  }
  return
}

// Return the anonymous value of a metric.
func (m *gmetric) GetValue() reflect.Value {
  return m.value
}

// Create a new ganglia metric using the specified format and
// with the parameters specified in a GMetricInfo structure.
func NewMetric(format GangliaMsgFormat, info ...GMetricInfo) (gm GMetric, err error) {
  var mid GangliaMetricId
  for _,i := range info {
    if i.Name != nil {
      mid.Name = string(i.Name)
      mid.Spoof = i.Spoof
      mid.Exists = true
    }
    if i.Host != nil {
      mid.Host = string(i.Host)
      mid.Spoof = i.Spoof
      mid.Exists = true
    }
  }
  if !mid.Exists {
    err = fmt.Errorf("no valid metric id for %v", format)
    return
  }
  m := &gmetric{GangliaMetricId:mid,
                gangliaMsg:gangliaMsg{formatIdentifier:format}}
  for _,i := range info {
    if i.Value != nil {
      err = m.setvalue(i.Value)
      if err != nil {
        log.Fatalf("err %v",err)
        return
      }
    }
    if i.Format != "" {
      m.SetFormat([]byte(i.Format))
    }
  }

  gm = m
  return
}

// Returns a printable "xmlish" form of a ganglia metric including
// any associated basic metadata.
func (m *gmetric) String() string {
  var metric string = m.FormatId().String()
  var attrs []string
  var metadata *GangliaMetadata

  metadata = m.GetMetadata()

  if m.GangliaMetricId.Exists && m.GangliaMetricId.Name != "" {
    attrs = append(attrs, fmt.Sprintf("name=\"%s\"",m.GangliaMetricId.Name))
  }

  if m.GangliaMetricId.Exists && m.GangliaMetricId.Host != "" {
    attrs = append(attrs, fmt.Sprintf("host=\"%s\"",m.GangliaMetricId.Host))
  }

  if m.value.IsValid() {
    var f string
    if m.fmt == "" {
      f = "value=\"%v\""
    } else {
      f = "value=\""+m.fmt+"\""
    }

    attrs = append(attrs, fmt.Sprintf(f,m.value.Interface()))
  }

  t := m.value.Type()
  if metadata != nil && metadata.Type != "" {
    attrs = append(attrs, fmt.Sprintf("type=\"%s\"",metadata.Type))
  } else if t != nil {
    attrs = append(attrs, fmt.Sprintf("type=\"%v\"",t))
  }

  if metadata != nil {
    if metadata.Units != "" {
      attrs = append(attrs, fmt.Sprintf("units=\"%s\"",metadata.Units))
    }
    if metadata.Slope > GangliaSlope(0) {
      attrs = append(attrs, fmt.Sprintf("slope=\"%s\"",metadata.Slope.String()))
    }
    attrs = append(attrs, fmt.Sprintf("tmax=\"%v\"",metadata.Tmax))
    attrs = append(attrs, fmt.Sprintf("dmax=\"%v\"",metadata.Dmax))
  }

  return "<" + metric + " " + strings.Join(attrs," ") + "/>"
}

// Start a simple network client which will pass packets to
// an xdr decoder.
func Client(addr string, mcast bool, xdr_chan chan []byte) {
  wg := sync.WaitGroup{}

  maddr,err := net.ResolveUDPAddr("udp4",addr)
  if err != nil {
    log.Fatalf("cannot resolve %v",addr,err)
  }
  wg.Add(1)
  defer close(xdr_chan)
  defer wg.Wait()
  go func(addr *net.UDPAddr) {
    var conn *net.UDPConn
    var err error

    defer wg.Done()
    defer func() {
      err := recover()
      if err != nil {
        log.Fatalf("CLIENT PANIC: %v",err)
      }
    }()

    if mcast {
      conn,err = net.ListenMulticastUDP("udp4",nil,addr)
    } else {
      conn,err = net.ListenUDP("udp4",addr)
    }
    if err != nil {
      log.Printf("network failure during listen: %v", err)
      return
    }
    log.Printf("Now listening for packets on %v", addr)
    defer conn.Close()
    for cnt := int(1); cnt > 0; cnt++ {
      var buf []byte = make([]byte, 1500)
      nbytes, saddr, err := conn.ReadFromUDP(buf)
      _ = saddr
      if err != nil {
        log.Printf("%d: read socket failure: %v", cnt, err)
        return
      }
      // log.Printf("%d: socket read: %v/%v bytes from %v",cnt,nbytes,len(buf),saddr)
      if nbytes > 0 {
        xdr_chan <- buf[:nbytes]
      }
    }
  }(maddr)
}

/*
func main() {
  var c chan []byte = make(chan []byte,1)
  var msgchan chan GangliaMetadataMessage

  metric,err := NewMetric(GMETRIC_DOUBLE,GMetricInfo{
      Name: []byte("cheese"),
      Value: ConstGangliaValue(GANGLIA_VALUE_DOUBLE,1),
      Format: "%0.5f",
    })
  if err != nil {
    log.Fatalf("cannot create new metric: %v", err)
  }

  log.Printf("hi: %v",metric)
  m := metric.(*gmetric)
  m.metadata = &GangliaMetadata{
                        Name: "cheese",
                        Units: "fucks/sec",
                    }
  log.Printf("hi: %v",metric)
  log.Printf("  ... hmmmmm value: %v", GANGLIA_VALUE_UNSIGNED_SHORT)

  msgchan = make(chan GangliaMetadataMessage,32)
  go func() {
    for {
      select {
      case msg := <-msgchan:
        if msg.IsMetadataDef() {
          md := msg.GetMetadata()
          //if md.metric_id == nil {
            md.metric_id = msg.MetricId()
          //}
          _,err := GangliaMetadataServer.Register(md)

          if err != nil {
            log.Printf("METADATA ERROR: %v", err)
          }
        } else {
          log.Printf("GOT %v",msg)
        }
      }
    }
  }()
  err = StartSocketReader(c,nil,msgchan)
  if err != nil {
    log.Fatalf("StartSocketReader: %v", err)
  }
  //server("0.0.0.0:8749",false,c)
  server("239.2.11.71:8649",true,c)
}
*/
