#ifndef GOLANG_GANGLIA_HELPER_H
#define GOLANG_GANGLIA_HELPER_H

#include <ganglia.h>
#include <gm_protocol.h>

extern void sanitize_metric_name(char *metric_name, int is_spoof_msg);
extern bool_t helper_init_xdr(XDR *x, Ganglia_msg_formats *id);
extern void helper_uninit_xdr(XDR *x, Ganglia_msg_formats *id);
extern int helper_free_xdr(XDR *x, Ganglia_msg_formats *id, void *msg);
extern size_t helper_perform_xdr(XDR *x, Ganglia_metadata_msg *fmsg,
                                         Ganglia_value_msg *vmsg,
                                         Ganglia_msg_formats *id);
extern void helper_destroy_xdr(XDR*);

/* union access helpers */
extern Ganglia_metadatadef *Ganglia_metadata_msg_u_gfull(Ganglia_metadata_msg*);
extern Ganglia_metadatareq *Ganglia_metadata_msg_u_grequest(Ganglia_metadata_msg*);
extern Ganglia_gmetric_ushort *Ganglia_value_msg_u_gu_short(Ganglia_value_msg*);
extern Ganglia_gmetric_short *Ganglia_value_msg_u_gs_short(Ganglia_value_msg*);
extern Ganglia_gmetric_int *Ganglia_value_msg_u_gs_int(Ganglia_value_msg*);
extern Ganglia_gmetric_uint *Ganglia_value_msg_u_gu_int(Ganglia_value_msg*);
extern Ganglia_gmetric_string *Ganglia_value_msg_u_gstr(Ganglia_value_msg*);
extern Ganglia_gmetric_float *Ganglia_value_msg_u_gf(Ganglia_value_msg*);
extern Ganglia_gmetric_double *Ganglia_value_msg_u_gd(Ganglia_value_msg*);

extern int helper_bool(void*);

extern size_t Ganglia_metadata_msg_size;
extern size_t Ganglia_metadata_val_size;
extern size_t XDR_size;

#endif /* GOLANG_GANGLIA_HELPER_H */
