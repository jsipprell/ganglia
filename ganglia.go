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
  "regexp"
)

var (
  typeMap map[MsgFormat] reflect.Kind
  revTypeMap map[reflect.Kind] MsgFormat
  stringType = reflect.TypeOf("")
  formatRe *regexp.Regexp
  GMetricTypeError = errors.New("Type not compatible with gmetric type")
  GMetricFormatError = errors.New("Cannot convert gmetric value to a formattable value")
  GMetricNoValueError = errors.New("Metric value is invalid")
  GMetadataNotKeyed = errors.New("No keyed or indexed metadata is available for this object yet")
)

func init() {
  typeMap = make(map[MsgFormat] reflect.Kind)
  revTypeMap = make(map[reflect.Kind] MsgFormat)

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

  formatRe = regexp.MustCompile("%([.\\d]+)?(?:h|l|ll)?([udf])")
}

// For use when constructing new metrics via NewMetric, all fields are optional.
type MetricInfo struct {
  Value interface{}
  Format string
  Host []byte
  Name []byte
  Spoof bool
}

// All mertrics and metadata must respond with an Id
type MetricType interface {
  MetricId() *MetricIdentifier
}

// All ganglia messages have an associated format identifer
type Identifier interface {
  FormatId() MsgFormat
}

// All ganglia objects that can be queried about metadata requests,
// whether they contain their own metadata or whether they define
// metadata. Finally, GetMetadata() can be used to acquire any
// such metadata
type MetadataQuery interface {
  KeyValueMetadataQuery
  IsRequest() bool
  HasMetadata() bool
  IsMetadataDef() bool
  GetMetadata() *Metadata
}

// KeyValueMetadataQuery is the interface for those objects that
// can return key value mapping objects for arbitrary metadata.
// Even though an object may implement this interface it still may
// return error under certain conditions
type KeyValueMetadataQuery interface {
  GetKeyValueMetadata() (KeyValueMetadata, error)
}

// The basic interface all messages will support when spit out
// by the xdr decoder.
type Message interface {
  MetadataQuery
  FormatId() MsgFormat
  MetricId() *MetricIdentifier
}

// All ganglia objects that contain actual metrics (rather than
// separate metadata definitions or requests) will support this
// interface.
type Metric interface {
  MetadataQuery
  FormatId() MsgFormat
  MetricId() *MetricIdentifier
  GetValue() reflect.Value
  String() string
}

type FormattedMetric interface {
  Metric
  FormatValue(...interface{}) (string,error)
  SetFormat([]byte)
}

// MetricIdentifier identifies a specific message, metric
// or metadata packet.
type MetricIdentifier struct {
  Host, Name string
  Spoof bool
  Exists bool
  id uid
}

// Return the canonical metric id for a message or metadata
// packet.
func (mid *MetricIdentifier) MetricId() *MetricIdentifier {
  return mid
}

type gangliaMsg struct {
  formatIdentifier MsgFormat
}

func (msg gangliaMsg) FormatId() (MsgFormat) {
  return msg.formatIdentifier
}

func (msg gangliaMsg) String() (string) {
  return msg.formatIdentifier.String()
}

func (msg *gangliaMsg) IsMetadataDef() bool {
  return false
}

func (sl Slope) String() (s string) {
  switch(sl) {
  case SLOPE_ZERO:
    s = "zero"
  case SLOPE_POSITIVE:
    s = "positive"
  case SLOPE_NEGATIVE:
    s = "negative"
  case SLOPE_BOTH:
    s = "both"
  case SLOPE_DERIVATIVE:
    s = "derivative"
  case SLOPE_UNSPECIFIED:
    s = "unspecified"
  default:
    s = fmt.Sprintf("unsupported_slope_%d",int(sl))
  }
  return
}

func (id MsgFormat) String() (s string) {
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

// Interface for metadata objects which can be referenced by key/value lookup.
// Objects supproting this interface can be automatically generated by
// MetadataDef.GetKeyValueMetadata() as long as such metadata has been seen.
type KeyValueMetadata interface {
  // Return the primary key for all metadata (if applicable) or a specific
  // key if indexed
  Key() ([]byte, error)
  // Return the value for a specific key which may not be the current object (!!)
  // Pass nil for the first arg to use the default key
  ValueFor([]byte) ([]byte, error)
  // Return a key/value mapping for all keyed/indexed metadata
  Mapping() (map[string] string)
}

// Basic ganglia metadata strucutre.
type Metadata struct {
  Type string
  Name string
  Units string
  Slope Slope
  Tmax uint
  Dmax uint

  extra []KeyValueMetadata
  metric_id *MetricIdentifier
}

// Create a unique copy of some piece of metadata.
// Note that the metric id is never copied and always shared
// by all copies.
func (md *Metadata) copy() (*Metadata) {
  var extra []KeyValueMetadata

  if md.extra != nil {
    extra = make([]KeyValueMetadata,len(md.extra),len(md.extra))
    copy(extra,md.extra)
  }
  return &Metadata{
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
type MetadataDef struct {
  gangliaMsg
  *MetricIdentifier
  metric Metadata
}

type mappedMetadata struct {
  *Metadata
}

func (mm *mappedMetadata) Key() (k []byte, err error) {
  if len(mm.Metadata.extra) > 0 {
    k,err = mm.Metadata.extra[0].Key()
  }
  return
}

func (mm *mappedMetadata) ValueFor(k []byte)  (v []byte, err error) {
  if len(mm.Metadata.extra) > 0 {
    v,err = mm.Metadata.extra[0].ValueFor(k)
  }
  return
}

func (mm *mappedMetadata) Mapping() (m map[string] string) {
  if len(mm.Metadata.extra) > 0 {
    m = mm.Metadata.extra[0].Mapping()
  }
  return
}

func (mdef *MetadataDef) GetKeyValueMetadata() (KeyValueMetadata, error) {
  if mdef.metric.extra == nil {
    return nil,GMetadataNotKeyed
  }
  return &mappedMetadata{Metadata:&mdef.metric},nil
}

func (md *Metadata) GetKeyValueMetadata() (KeyValueMetadata, error) {
  if md.extra == nil {
    return nil,GMetadataNotKeyed
  }
  return &mappedMetadata{Metadata:md},nil
}

// Returns the canonical metric id of a metadata update
func (mdef *MetadataDef) MetricId() (*MetricIdentifier) {
  if mdef.metric.metric_id != nil {
    return mdef.metric.metric_id
  }
  return mdef.MetricIdentifier
}

// Always returns true for a metdadate definition/update.
func (mdef *MetadataDef) HasMetadata() bool {
  return true
}

// Always returns true for a metdadate definition/update.
func (mdef *MetadataDef) IsMetadataDef() bool {
  return true
}

// Returns the actual metadata from a definition/update
func (mdef *MetadataDef) GetMetadata() *Metadata {
  return &mdef.metric
}

// Always returns false for a metadata definition/update
func (mdef *MetadataDef) IsRequest() bool {
  return false
}

// Requests metadata update from an agent.
type MetadataReq struct {
  gangliaMsg
  *MetricIdentifier
}

// Always returns true for a metadata request
func (mreq *MetadataReq) IsRequest() bool {
  return true
}

// Always returns false for a metadata request
func (mreq *MetadataReq) HasMetadata() bool {
  return false
}

// Always returns false for a metadata request
func (mreq *MetadataReq) IsMetadataDef() bool {
  return false
}

// Requests never have any metadata thus this will return nil.
func (mreq *MetadataReq) GetMetadata() *Metadata {
  return nil
}

func (mreq *MetadataReq) GetKeyValueMetadata() (KeyValueMetadata, error) {
  return nil, GMetadataNotKeyed
}

type gmetric struct {
  gangliaMsg
  MetricIdentifier
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
func (m *gmetric) GetMetadata() (md *Metadata) {
  if m.metadata != nil {
    md = m.metadata.(*Metadata)
  }
  return
}

func (m *gmetric) GetKeyValueMetadata() (md KeyValueMetadata, err error) {
  if m.metadata != nil {
    md,err = m.metadata.(*Metadata).GetKeyValueMetadata()
  } else {
    err = GMetadataNotKeyed
  }
  return
}

// Always returns false for a ganglia metric.
func (m *gmetric) IsRequest() bool {
  return false
}

// Return the canonical metric id.
func (m *gmetric) MetricId() (*MetricIdentifier) {
  return &m.MetricIdentifier
}

// Set the printf() style format string. Empty
// strings or nil will use "%v" from go parlance.
func (m *gmetric) SetFormat(f []byte) {
  newfmt := formatRe.ReplaceAll(f,[]byte("%$1$2"))
  l := len(newfmt)
  if l > 0 && newfmt[l-1] == 'u' {
    newfmt[l-1] = 'd'
  }
  m.fmt = string(newfmt)
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
// with the parameters specified in a MetricInfo structure.
func NewMetric(format MsgFormat, info ...MetricInfo) (gm Metric, err error) {
  var mid MetricIdentifier
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
  m := &gmetric{MetricIdentifier:mid,
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

// Returns the value of a metric formatted per its ganglia specification
func (m *gmetric) FormatValue(args ...interface{}) (s string, err error) {
  var format []byte
  if len(args) > 0 {
    format = []byte(args[0].(string))
  }
  if !m.value.IsValid() {
    err = GMetricNoValueError
  } else if m.fmt == "" {
    if m.value.Type().ConvertibleTo(stringType) {
      s = m.value.Convert(stringType).String()
    }
  } else if m.fmt == "%v" {
    s = fmt.Sprintf(m.fmt,m.value.Interface())
  } else if m.IsKind(reflect.Float32,reflect.Float64) {
    s = fmt.Sprintf(m.fmt,m.value.Float())
  } else if m.IsKind(reflect.Int32,reflect.Int16,reflect.Int64,reflect.Int) {
    s = fmt.Sprintf(m.fmt,int(m.value.Int()))
  } else if m.IsKind(reflect.Uint32,reflect.Uint16,reflect.Uint64,reflect.Uint) {
    s = fmt.Sprintf(m.fmt,uint(m.value.Uint()))
  } else if m.value.Type().ConvertibleTo(stringType) {
    s = fmt.Sprintf(m.fmt,m.value.Convert(stringType).String())
  } else {
    err = GMetricFormatError
  }

  if err == nil && format != nil {
    args[0] = s
    s = fmt.Sprintf(string(format),args...)
  }
  return
}

// Returns a printable "xmlish" form of a ganglia metric including
// any associated basic metadata.
func (m *gmetric) String() string {
  var metric string = m.FormatId().String()
  var attrs []string
  var metadata *Metadata

  metadata = m.GetMetadata()

  if metadata != nil {
    kv,err := metadata.GetKeyValueMetadata()
    if err == nil {
      mapping := kv.Mapping()
      if mapping != nil {
        if group,ok := mapping["GROUP"]; ok {
          metric = string(group)
        }
      }
    }
  }

  if m.MetricIdentifier.Exists && m.MetricIdentifier.Name != "" {
    attrs = append(attrs, fmt.Sprintf("name=\"%s\"",m.MetricIdentifier.Name))
  }

  if m.MetricIdentifier.Exists && m.MetricIdentifier.Host != "" {
    attrs = append(attrs, fmt.Sprintf("host=\"%s\"",m.MetricIdentifier.Host))
  }

  s,err  := m.FormatValue("value=\"%s\"")
  if err == nil {
    attrs = append(attrs, s)
  }

  t := m.value.Type()
  if metadata != nil && metadata.Type != "" {
    attrs = append(attrs, fmt.Sprintf("type=\"%s\"",metadata.Type))
  } else if t != nil {
    attrs = append(attrs, fmt.Sprintf("type=\"%v\"",t))
  }

  if m.fmt != "" && m.fmt != "%v" {
    attrs = append(attrs, fmt.Sprintf("format=\"%s\"",m.fmt))
  }

  if metadata != nil {
    if metadata.Units != "" {
      attrs = append(attrs, fmt.Sprintf("units=\"%s\"",metadata.Units))
    }
    if metadata.Slope > Slope(0) {
      attrs = append(attrs, fmt.Sprintf("slope=\"%s\"",metadata.Slope.String()))
    }
    attrs = append(attrs, fmt.Sprintf("tmax=\"%v\"",metadata.Tmax))
    attrs = append(attrs, fmt.Sprintf("dmax=\"%v\"",metadata.Dmax))
  }

  return "<" + metric + " " + strings.Join(attrs," ") + "/>"
}

// Start a simple network client which will pass packets to
// an xdr decoder. The client returns a channel which if closed will terminate the client and
// a waitgroup that can be used to wait for this termination to complete. Errors returned
// are almost entirely due to underlying network errors.
//
// The address argument should be either a multicast:port pair, an interface address:port pair
// or 0.0.0.0:port/[::]:port for ipv4/ipv6 "any interface" mode.
func Client(addr string, xdr_chan chan []byte) (quit chan struct{}, wg *sync.WaitGroup, err error) {
  var conn *net.UDPConn
  var maddr *net.UDPAddr
  maddr,err = net.ResolveUDPAddr("udp",addr)
  if err != nil {
    return
  }

  if maddr.IP.IsMulticast() {
    conn,err = net.ListenMulticastUDP("udp",nil,maddr)
  } else {
    conn,err = net.ListenUDP("udp",maddr)
  }

  if err != nil {
    return
  }
  quit = make(chan struct{})
  wg = &sync.WaitGroup{}
  wg.Add(1)
  go func() {
    var err error

    defer wg.Done()
    defer func() {
      select {
      case <-quit:
      default:
        close(quit)
      }
    }()
    defer func() {
      err := recover()
      if err != nil {
        log.Fatalf("CLIENT PANIC: %v",err)
      }
    }()

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
  }()

  wg.Add(1)
  go func() {
    defer wg.Done()
    defer conn.Close()
    <-quit
  }()
  return
}

// vi: set sts=2 sw=2 ai et tw=0 syntax=go:
