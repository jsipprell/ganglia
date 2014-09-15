Ganglia Go Package
==================

A small go package that provides basic access to Ganglia network metrics.

Currently this package only permits decoding of Ganglia XDR packets/streams as
the immediate need was for decode only rather than encode. However, I fully
intend to add an encoder in the near future.

This package require cgo and an install libganglia (probably at least version
3.4.0).

The package uses pkg-config however ganglia does *not*. To get around this a
````make-pkgconfig.sh```` script has been supplied. To use:

    ./make-pkgconfig.sh > ganglia.pc
    # assuming all went well
    export PKG_CONFIG_PATH=`pwd`
    go install

