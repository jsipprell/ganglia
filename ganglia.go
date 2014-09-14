// cgo interface against libganglia
package main

/*
#cgo CFLAGS: -I/nm/local/include -I/usr/local/include
#cgo LDFLAGS: -L/nm/local/lib64 -L/usr/local/lib64 -lganglia

#include "helper.h"
*/
import "C"

import (
  "strings"
  "errors"
  "reflect"
  "fmt"
  "log"
)

type GangliaMsgFormat uint16
type GangliaSlope uint

const (
  GANGLIA_SLOPE_ZERO          GangliaSlope = 1 << iota
  GANGLIA_SLOPE_POSITIVE
  GANGLIA_SLOPE_NEGATIVE
  GANGLIA_SLOPE_BOTH
  GANGLIA_SLOPE_UNSPECTIFIED
  GANGLIA_SLOPE_DERIVATIVE
)
const GANGLIA_MAX_MESSAGE_LEN int = 1500

const (
  GMETADATA_FULL GangliaMsgFormat = 127 + iota
  GMETRIC_USHORT
  GMETRIC_SHORT
  GMETRIC_INT
  GMETRIC_UINT
  GMETRIC_STRING
  GMETRIC_FLOAT
  GMETRIC_DOUBLE
  GMETADATA_REQUEST
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

type GangliaMessage interface {
  Id() GangliaMsgFormat
}

type GangliaMetadataContainer interface {
  HasMetadata() bool
  GetMetadata() *GangliaMetadata
}

type GangliaMetadataMessage interface {
  GangliaMetadataContainer
  Id() GangliaMsgFormat
  MetricId() *GangliaMetricId
  IsRequest() bool
}

type GMetric interface {
  GangliaMetadataContainer
  Id() GangliaMsgFormat
  MetricId() *GangliaMetricId
  GetValue() reflect.Value
  SetFormat(f []byte)
  String() string
}

type GangliaMetricId struct {
  Host, Name string
  Spoof bool
  Exists bool
}

type gangliaBaseMessage struct {
  formatIdentifier GangliaMsgFormat
}

func (msg gangliaBaseMessage) Id() (GangliaMsgFormat) {
  return msg.formatIdentifier
}

func (msg gangliaBaseMessage) String() (string) {
  return msg.formatIdentifier.String()
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
  case GANGLIA_SLOPE_UNSPECTIFIED:
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

type GangliaMetadata struct {
  Type string
  Name string
  Units string
  Slope GangliaSlope
  Tmax, Dmax uint
  extra []KeyValueMetadata
}

type GangliaMetadataDef struct {
  gangliaBaseMessage
  *GangliaMetricId
  metric GangliaMetadata
}

func (mdef *GangliaMetadataDef) HasMetadata() bool {
  return true
}

func (mdef *GangliaMetadataDef) GetMetadata() *GangliaMetadata {
  return &mdef.metric
}

type GangliaMetadataReq struct {
  gangliaBaseMessage
  *GangliaMetricId
}

func (mreq *GangliaMetadataReq) HasMetadata() bool {
  return false
}

func (mreq *GangliaMetadataReq) GetMetadata() *GangliaMetadata {
  return nil
}

type gmetric struct {
  gangliaBaseMessage
  *GangliaMetricId
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

func (m *gmetric) MetricId() (*GangliaMetricId) {
  return m.GangliaMetricId
}

func (m *gmetric) SetFormat(f []byte) {
  m.fmt = string(f)
}

func (m *gmetric) setvalue(v interface{}) (err error) {
  t := reflect.TypeOf(v)
  id,ok := revFormatMap[t.Kind()]
  if !ok || id != m.Id() {
    if !ok {
      err = fmt.Errorf("Type of %v (%v) does not match kind %v",v,t,t.Kind())
    } else {
      err = fmt.Errorf("Ganglia type identifier %v does not match %v/%v",
                       m.Id(),id,t.Kind())
    }
    //err = GMetricTypeError
    return
  }

  if !m.value.IsValid() {
    m.value = valueof(v)
  } else {
    m.value.Set(valueof(v))
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

func (m *gmetric) GetValue() reflect.Value {
  return m.value
}

func NewMetric(format GangliaMsgFormat, info ...GMetricInfo) (gm GMetric, err error) {
  m := &gmetric{GangliaMetricId:&GangliaMetricId{},
               gangliaBaseMessage:gangliaBaseMessage{formatIdentifier:format}}

  for _,i := range info {
    if i.Host != nil && i.Name != nil {
      m.Host = string(i.Host)
      m.Name = string(i.Name)
      m.Spoof = i.Spoof
    }
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

func (m *gmetric) String() string {
  var metric string = m.Id().String()
  var attrs []string
  var metadata *GangliaMetadata

  metadata = m.GetMetadata()

  if m.GangliaMetricId.Exists && m.GangliaMetricId.Name != "" && (metadata == nil || metadata.Name == "") {
    attrs = append(attrs, fmt.Sprintf("name=\"%s\"",m.GangliaMetricId.Name))
  } else if metadata != nil && metadata.Name != "" {
    attrs = append(attrs, fmt.Sprintf("name=\"%s\"",metadata.Name))
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

    if metadata == nil || metadata.Type == "" {
      t := m.value.Type()
      if t != nil {
        attrs = append(attrs, fmt.Sprintf("type=\"%v\"",t))
      }
    }
  }

  if metadata != nil {
    if metadata.Type != "" {
      attrs = append(attrs, fmt.Sprintf("type=\"%s\"",metadata.Type))
    }
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

func main() {
  metric,err := NewMetric(GMETRIC_FLOAT,GMetricInfo{
      Value: float32(4.4),
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
}
