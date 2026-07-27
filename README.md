# raknet-python

Python bindings for the [RakNet](https://github.com/facebookarchive/RakNet) networking library, built with [nanobind](https://github.com/wjakob/nanobind) and [conan-py-build](https://github.com/conan-io/conan-py-build).

## Build

The `raknet` Conan recipe lives on the `endstone` remote:

```shell
conan remote add endstone https://conan.cloudsmith.io/endstone/conan/
```

Then build and install the wheel:

```shell
pip install .
```
