/* cgo helper routines for decoding ganglia xdr */

#include "helper.h"
#include "_cgo_export.h"

size_t Ganglia_metadata_msg_size = sizeof(Ganglia_metadata_msg);
size_t Ganglia_metadata_val_size = sizeof(Ganglia_value_msg);
size_t XDR_size = sizeof(XDR);

static void emit_debug(const char *s)
{
  GoString msg;

  msg.p = (char*)s;
  msg.n = strlen(s);
  helper_debug(msg);
}

#if 0
#define DEBUG(s) emit_debug((s))
#else
#define DEBUG(s) (void)s
#endif

void sanitize_metric_name(char *metric_name, int is_spoof_msg)
{
    if (metric_name == NULL) return;
    if (strlen(metric_name) < 1) return;
    char *p = metric_name;
    while (p < (metric_name + strlen(metric_name))) {
        if (
               !(*p >= 'A' && *p <= 'Z')
            && !(*p >= 'a' && *p <= 'z')
            && !(*p >= '0' && *p <= '9')
            && (*p != '_')
            && (*p != '-')
            && (*p != '.')
            && (*p == ':' && !is_spoof_msg)
            && (*p != '\0')
           ) {
            *p = '_';
        }
        p++;
    }
}

int helper_bool(void *v)
{
  return (int)*((bool_t*)v);
}

bool_t helper_init_xdr(XDR *x, Ganglia_msg_formats *id)
{
  bool_t ok;

  u_int save = xdr_getpos(x);
  ok = xdr_Ganglia_msg_formats(x,id);
  xdr_setpos(x,save);
  if(ok)
    DEBUG("xdr init ok");
  return ok;
}

void helper_uninit_xdr(XDR *x, Ganglia_msg_formats *id)
{
  xdr_free((xdrproc_t)xdr_Ganglia_msg_formats, (char*)id);
}

void helper_destroy_xdr(XDR *x) {
  xdr_destroy(x);
}

int helper_free_xdr(XDR *x, Ganglia_msg_formats *id, void *msg) {
  int count = 0;
  if(id) {
    switch(*id) {
      case gmetadata_request:
      case gmetadata_full:
        xdr_free((xdrproc_t)xdr_Ganglia_metadata_msg, (char*)msg);
        count++;
        break;
      case gmetric_ushort:
      case gmetric_short:
      case gmetric_int:
      case gmetric_uint:
      case gmetric_string:
      case gmetric_float:
      case gmetric_double:
        xdr_free((xdrproc_t)xdr_Ganglia_value_msg, (char*)msg);
        count++;
        break;
      default:
        break;
    }
  }
  return count;
}

Ganglia_metadatadef *Ganglia_metadata_msg_u_gfull(Ganglia_metadata_msg *fmsg)
{
  return &fmsg->Ganglia_metadata_msg_u.gfull;
}

Ganglia_metadatareq *Ganglia_metadata_msg_u_grequest(Ganglia_metadata_msg *fmsg)
{
  return &fmsg->Ganglia_metadata_msg_u.grequest;
}

Ganglia_gmetric_ushort *Ganglia_value_msg_u_gu_short(Ganglia_value_msg *msg)
{
  return &msg->Ganglia_value_msg_u.gu_short;
}

Ganglia_gmetric_short *Ganglia_value_msg_u_gs_short(Ganglia_value_msg *msg)
{
  return &msg->Ganglia_value_msg_u.gs_short;
}

Ganglia_gmetric_int *Ganglia_value_msg_u_gs_int(Ganglia_value_msg *msg)
{
  return &msg->Ganglia_value_msg_u.gs_int;
}

Ganglia_gmetric_uint *Ganglia_value_msg_u_gu_int(Ganglia_value_msg *msg)
{
  return &msg->Ganglia_value_msg_u.gu_int;
}

Ganglia_gmetric_string *Ganglia_value_msg_u_gstr(Ganglia_value_msg *msg)
{
  return &msg->Ganglia_value_msg_u.gstr;
}

Ganglia_gmetric_float *Ganglia_value_msg_u_gf(Ganglia_value_msg *msg)
{
  return &msg->Ganglia_value_msg_u.gf;
}

Ganglia_gmetric_double *Ganglia_value_msg_u_gd(Ganglia_value_msg *msg)
{
  return &msg->Ganglia_value_msg_u.gd;
}

size_t helper_perform_xdr(XDR *x, Ganglia_metadata_msg *fmsg,
                       Ganglia_value_msg *vmsg,
                       Ganglia_msg_formats *id)
{
  u_int pos;
  char buf[1024];
  bool_t ret;
  int count = 0;

  DEBUG("starting xdr 1");
  memset(fmsg, 0, sizeof(Ganglia_metadata_msg));
  memset(vmsg, 0, sizeof(Ganglia_value_msg));

  pos = xdr_getpos(x);
  sprintf(buf,"XDR: id=%d, pos=%d",(int)(*id),(int)pos);
  switch(*id) {
    case gmetadata_request:
      ret = xdr_Ganglia_metadata_msg(x, fmsg);
      if (ret) {
        sanitize_metric_name(fmsg->Ganglia_metadata_msg_u.grequest.metric_id.name,
                             fmsg->Ganglia_metadata_msg_u.grequest.metric_id.spoof);
        count++;
      } else {
        xdr_setpos(x,pos);
      }
      break;
    case gmetadata_full:
      ret = xdr_Ganglia_metadata_msg(x, fmsg);
      if (ret) {
        sanitize_metric_name(fmsg->Ganglia_metadata_msg_u.gfull.metric_id.name,
                             fmsg->Ganglia_metadata_msg_u.gfull.metric_id.spoof);
        count++;
      } else {
        xdr_setpos(x,pos);
      }
      break;
    case gmetric_ushort:
    case gmetric_short:
    case gmetric_int:
    case gmetric_uint:
    case gmetric_string:
    case gmetric_float:
    case gmetric_double:
      ret = xdr_Ganglia_value_msg(x, vmsg);
      if(ret) {
        sanitize_metric_name(vmsg->Ganglia_value_msg_u.gstr.metric_id.name, vmsg->Ganglia_value_msg_u.gstr.metric_id.spoof);
        count++;
      } else {
        xdr_setpos(x,pos);
      }
      break;
    default:
      DEBUG(buf);
      DEBUG("bad id");
      break;
  }
  sprintf(buf,"finished xdr, count=%d, bytes=%d",count,xdr_getpos(x)-pos);
  DEBUG(buf);
  return xdr_getpos(x) - pos;
}
