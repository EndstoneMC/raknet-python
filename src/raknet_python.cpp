#include <nanobind/nanobind.h>

#include <GetTime.h>

namespace nb = nanobind;

NB_MODULE(_raknet, m)
{
    m.doc() = "Python bindings for the RakNet networking library";

    m.def("time", [] { return RakNet::GetTime(); });
    m.def("time_ms", [] { return RakNet::GetTimeMS(); });
}
