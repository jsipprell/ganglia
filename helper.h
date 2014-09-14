#ifndef GOLANG_GANGLIA_HELPER_H
#define GOLANG_GANGLIA_HELPER_H

#include <ganglia.h>

extern void sanitize_metric_name(char *metric_name, int is_spoof_msg);
extern void helper_init_xdr(XDR *x, Ganglia_msg_formats *id, int pos);
extern int helper_free_xdr(XDR *x, Ganglia_msg_formats *id, void *msg);
extern int helper_perform_xdr(XDR *x, int *xdr_cleanup, Ganglia_metadata_msg *fmsg,
                             Ganglia_value_msg *vmsg,
                             Ganglia_msg_formats *id);
extern void helper_destroy_xdr(XDR*);

#endif /* GOLANG_GANGLIA_HELPER_H */
