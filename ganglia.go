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
  typeMap map[GangliaMsgFormat] reflect.Kind
  revTypeMap map[reflect.Kind] GangliaMsgFormat
  GMetricTypeError = errors.New("Type not compatible with gmetric type")
)

func init() {
  typeMap = make(map[GangliaMsgFormat] reflect.Kind)
  revTypeMap = make(map[reflect.Kind] GangliaMsgFormat)

  typeMap[GMETRIC_USHORT] = reflect.Uint16
  revTypeMap[reflect.Uint16] = GMETRIC_USHORT
  typeMap[GMETRIC_SHORT] = reflect.Int16
  revTypeMap[reflect.Int16] = GMETRIC_SHORT
  typeMap[GMETRIC_INT] = reflect.Int32
  revTypeMap[reflect.Int32] = GMETRIC_INT
  revTypeMap[reflect.Int] = GMETRIC_INT
  typeMap[GMETRIC_UINT] = reflect.Uint32
  revTypeMap[reflect.Uint32] = GMETRIC_UINT
  revTypeMap[reflect.Uint] = GMETRIC_UINT
  typeMap[GMETRIC_STRING] = reflect.String
  revTypeMap[reflect.String] = GMETRIC_STRING
  typeMap[GMETRIC_FLOAT] = reflect.Float32
  revTypeMap[reflect.Float32] = GMETRIC_FLOAT
  typeMap[GMETRIC_DOUBLE] = reflect.Float64
  revTypeMap[reflect.Float64] = GMETRIC_DOUBLE
}

// For use when constructing new metrics via NewMetric, all fields are optional.
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
type GangliaIdentifier interface {
  FormatId() GangliaMsgFormat
}

// All ganglia objects that can be queried about metadata requests,
// whether they contain their own metadata or whether they define
// metadata. Finally, GetMetadata() can be used to acquire any
// such metadata
type GangliaMetadataHandler interface {
  IsRequest() bool
  HasMetadata() bool
  IsMetadataDef() bool
  GetMetadata() *GangliaMetadata
}

// The basic interface all messages will support when spit out
// by the xdr decoder.
type GangliaMessage interface {
  GangliaMetadataHandler
  FormatId() GangliaMsgFormat
  MetricId() *GangliaMetricId
}

// All ganglia objects that contain actual metrics (rather than
// separate metadata definitions or requests) will support this
// interface.
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

func (msg gangliaMsg) FormatId() (GangliaMsgFormat) {
  return msg.formatIdentifier
}

func (msg gangliaMsg) String() (string) {
  return msg.formatIdentifier.String()
}

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

// Basic ganglia metadata strucutre.
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
// Note that the metric id is never copied and always shared
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

// Always returns true for a metdadate definition/update.
func (mdef *GangliaMetadataDef) HasMetadata() bool {
  return true
}

// Always returns true for a metdadate definition/update.
func (mdef *GangliaMetadataDef) IsMetadataDef() bool {
  return true
}

// Returns the actual metadata from a definition/update
func (mdef *GangliaMetadataDef) GetMetadata() *GangliaMetadata {
  return &mdef.metric
}

// Always returns false for a metadata definition/update
func (mdef *GangliaMetadataDef) IsRequest() bool {
  return false
}

// Requests metadata update from an agent.
type GangliaMetadataReq struct {
  gangliaMsg
  *GangliaMetricId
}

// Always returns true for a metadata request
func (mreq *GangliaMetadataReq) IsRequest() bool {
  return true
}

// Always returns false for a metadata request
func (mreq *GangliaMetadataReq) HasMetadata() bool {
  return false
}

// Always returns false for a metadata request
func (mreq *GangliaMetadataReq) IsMetadataDef() bool {
  return false
}

// Requests never have any metadata thus this will return nil.
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

// Returns true if a metric has associated metadata. Metadata
// is automatically associated when available via the metadata
// server.
func (m *gmetric) HasMetadata() (r bool) {
  if m.metadata != nil {
    r = true
  }
  return
}

// Returns the metadata associated with a metric.
func (m *gmetric) GetMetadata() (md *GangliaMetadata) {
  if m.metadata != nil {
    md = m.metadata.(*GangliaMetadata)
  }
  return
}

// Always returns false for a ganglia metric.
func (m *gmetric) IsRequest() bool {
  return false
}

// Return the canonical metric id.
func (m *gmetric) MetricId() (*GangliaMetricId) {
  return &m.GangliaMetricId
}

// Set the printf() style format string. Empty
// strings or nil will use "%v" from go parlance.
func (m *gmetric) SetFormat(f []byte) {
  m.fmt = string(f)
}

type valueTypes interface {
  Elem() reflect.Value
  CanSet() bool
  CanInterface() bool
  CanAddr() bool
}

// Coerce a go value to fit a metric type. Retunrs
// an error if this is not possible.
func (m *gmetric) setvalue(v interface{}) (err error) {
  var t reflect.Type
  var V reflect.Value
  V,ok := v.(reflect.Value)
  if !ok {
    V = reflect.ValueOf(v)
  }

  t = V.Type()
  id,ok := revTypeMap[t.Kind()]
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

// Test a series of go type kinds to see if any of them
// are compatible with a metric.
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

// Return the anonymous value of a metric as a reflect.Value.
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

  maddr,err := net.ResolveUDPAddr("udp",addr)
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
      conn,err = net.ListenMulticastUDP("udp",nil,addr)
    } else {
      conn,err = net.ListenUDP("udp",addr)
    }
    if err != nil {
      log.Printf("network failure during listen: %v", err)
      return
    }
    log.Printf("Now listening for packets on %v", addr)
    defer conn.Close()
    for cnt := int(1); cnt > 0; cnt++ {
      var buf []byte = make([]byte, GANGLIA_MAX_MESSAGE_LEN, GANGLIA_MAX_MESSAGE_LEN)
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

// vi: set sts=2 sw=2 ai et tw=0 syntax=go:
