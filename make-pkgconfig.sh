#!/bin/bash
#
# ganglia doesn't come with a pkg-config file but those are realistically the only way to dynamically
# configure cgo bindings. Run this script, which will use the ganglia-config script, to output a
# pkg-config pc file to stdout. Save this in one of pkg-config's searchable directories alas:
#
# ./make-pkgconfig.sh > /usr/local/lib/pkgconfig/ganglia.pc

if ! gcfg=$(which --skip-alias --skip-functions ganglia-config 2>/dev/null); then
  echo "Couldn't find ganglia-config, is it in your path?" >&2
  exit 1
fi

if ! awk=$(which --skip-alias --skip-functions awk 2>/dev/null); then
  if ! awk=$(which --skip-alias --skip-functions gawk 2>/dev/null); then
    echo "Couldn't find awk or gawk in your path." >&2
    exit 2
  fi
fi

gcfg_ldflags="$($gcfg --all --ldflags)"
cat - <<__PC_EOF
prefix=$($gcfg --prefix)
exec_prefix=$($gcfg --exec-prefix)
libdir=$($gcfg --libdir)
includedir=$($gcfg --includedir)
bindir=$($gcfg --bindir)
datarootdir=$($gcfg --datarootdir)
version=$($gcfg --version | $awk '{ print $NF; }')

Name: ganglia
Description: Scalable cluster metrics collection and statistics monitoring
Version: \${version}
Libs: $gcfg_ldflags${gcfg_ldflags+ }$($gcfg --libs)
Cflags: $($gcfg --all --cflags)
__PC_EOF
