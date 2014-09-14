/* cgo helper routines for decoding ganglia xdr */

#include "helper.h"

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

void helper_init_xdr(XDR *x, Ganglia_msg_formats *id, int pos)
{
  xdr_Ganglia_msg_formats(x,id);
  xdr_setpos(x,pos);
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

int helper_perform_xdr(XDR *x, int *xdr_cleanup, Ganglia_metadata_msg *fmsg,
                       Ganglia_value_msg *vmsg,
                       Ganglia_msg_formats *id)
{
  bool_t ret;
  int count = 0;

  memset(&fmsg, 0, sizeof(Ganglia_metadata_msg));
  memset(&vmsg, 0, sizeof(Ganglia_value_msg));

  switch(*id) {
    case gmetadata_request:
      (*xdr_cleanup)++;
      ret = xdr_Ganglia_metadata_msg(x, fmsg);
      sanitize_metric_name(fmsg->Ganglia_metadata_msg_u.grequest.metric_id.name,
                             fmsg->Ganglia_metadata_msg_u.grequest.metric_id.spoof);
      if (!ret) {
        if(*xdr_cleanup > 0) {
          xdr_free((xdrproc_t)xdr_Ganglia_metadata_msg,(char*)fmsg);
          (*xdr_cleanup)--;
        }
        break;
      }
      count++;
      break;
    case gmetadata_full:
      (*xdr_cleanup)++;
      ret = xdr_Ganglia_metadata_msg(x, fmsg);
      sanitize_metric_name(fmsg->Ganglia_metadata_msg_u.gfull.metric_id.name,
                             fmsg->Ganglia_metadata_msg_u.gfull.metric_id.spoof);
      if (!ret) {
        if(*xdr_cleanup > 0) {
          xdr_free((xdrproc_t)xdr_Ganglia_metadata_msg,(char*)fmsg);
          (*xdr_cleanup)--;
        }
        break;
      }
      count++;
      break;
    case gmetric_ushort:
    case gmetric_short:
    case gmetric_int:
    case gmetric_uint:
    case gmetric_string:
    case gmetric_float:
    case gmetric_double:
      (*xdr_cleanup)++;
      ret = xdr_Ganglia_value_msg(x, vmsg);
      sanitize_metric_name(vmsg->Ganglia_value_msg_u.gstr.metric_id.name, vmsg->Ganglia_value_msg_u.gstr.metric_id.spoof);
      if(!ret) {
        if(*xdr_cleanup > 0) {
          xdr_free((xdrproc_t)xdr_Ganglia_value_msg, (char*)vmsg);
          (*xdr_cleanup)--;
        }
        break;
      }
      count++;
      break;
    default:
      break;
  }
  return count;
}
