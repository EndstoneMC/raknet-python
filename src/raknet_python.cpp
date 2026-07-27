#include <DS_List.h>
#include <GetTime.h>
#include <MessageIdentifiers.h>
#include <PacketPriority.h>
#include <RakNetStatistics.h>
#include <RakNetTypes.h>
#include <RakNetVersion.h>
#include <RakPeerInterface.h>

#include <cstring>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <nanobind/nanobind.h>
#include <nanobind/operators.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/pair.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

namespace nb = nanobind;

namespace {

struct Packet {
    RakNet::SystemAddress system_address;
    RakNet::RakNetGUID guid;
    unsigned int length;
    RakNet::BitSize_t bit_size;
    nb::bytes data;
    bool was_generated_locally;
};

class RakPeer {
public:
    RakPeer() : peer_(RakNet::RakPeerInterface::GetInstance()) {}
    ~RakPeer() { RakNet::RakPeerInterface::DestroyInstance(peer_); }
    RakPeer(const RakPeer &) = delete;
    RakPeer &operator=(const RakPeer &) = delete;
    RakNet::RakPeerInterface *operator->() const { return peer_; }

private:
    RakNet::RakPeerInterface *peer_;
};

char delineator(const std::string &s)
{
    return s.empty() ? '|' : s[0];
}

}  // namespace

NB_MODULE(_raknet, m)
{
    m.doc() = "Python bindings for the RakNet networking library";

    m.attr("RAKNET_VERSION") = RAKNET_VERSION;
    m.attr("RAKNET_PROTOCOL_VERSION") = RAKNET_PROTOCOL_VERSION;

    nb::enum_<RakNet::StartupResult>(m, "StartupResult")
        .value("RAKNET_STARTED", RakNet::RAKNET_STARTED)
        .value("RAKNET_ALREADY_STARTED", RakNet::RAKNET_ALREADY_STARTED)
        .value("INVALID_SOCKET_DESCRIPTORS", RakNet::INVALID_SOCKET_DESCRIPTORS)
        .value("INVALID_MAX_CONNECTIONS", RakNet::INVALID_MAX_CONNECTIONS)
        .value("SOCKET_FAMILY_NOT_SUPPORTED", RakNet::SOCKET_FAMILY_NOT_SUPPORTED)
        .value("SOCKET_PORT_ALREADY_IN_USE", RakNet::SOCKET_PORT_ALREADY_IN_USE)
        .value("SOCKET_FAILED_TO_BIND", RakNet::SOCKET_FAILED_TO_BIND)
        .value("SOCKET_FAILED_TEST_SEND", RakNet::SOCKET_FAILED_TEST_SEND)
        .value("PORT_CANNOT_BE_ZERO", RakNet::PORT_CANNOT_BE_ZERO)
        .value("FAILED_TO_CREATE_NETWORK_THREAD", RakNet::FAILED_TO_CREATE_NETWORK_THREAD)
        .value("COULD_NOT_GENERATE_GUID", RakNet::COULD_NOT_GENERATE_GUID)
        .value("STARTUP_OTHER_FAILURE", RakNet::STARTUP_OTHER_FAILURE);

    nb::enum_<RakNet::ConnectionAttemptResult>(m, "ConnectionAttemptResult")
        .value("CONNECTION_ATTEMPT_STARTED", RakNet::CONNECTION_ATTEMPT_STARTED)
        .value("INVALID_PARAMETER", RakNet::INVALID_PARAMETER)
        .value("CANNOT_RESOLVE_DOMAIN_NAME", RakNet::CANNOT_RESOLVE_DOMAIN_NAME)
        .value("ALREADY_CONNECTED_TO_ENDPOINT", RakNet::ALREADY_CONNECTED_TO_ENDPOINT)
        .value("CONNECTION_ATTEMPT_ALREADY_IN_PROGRESS", RakNet::CONNECTION_ATTEMPT_ALREADY_IN_PROGRESS)
        .value("SECURITY_INITIALIZATION_FAILED", RakNet::SECURITY_INITIALIZATION_FAILED);

    nb::enum_<RakNet::ConnectionState>(m, "ConnectionState")
        .value("IS_PENDING", RakNet::IS_PENDING)
        .value("IS_CONNECTING", RakNet::IS_CONNECTING)
        .value("IS_CONNECTED", RakNet::IS_CONNECTED)
        .value("IS_DISCONNECTING", RakNet::IS_DISCONNECTING)
        .value("IS_SILENTLY_DISCONNECTING", RakNet::IS_SILENTLY_DISCONNECTING)
        .value("IS_DISCONNECTED", RakNet::IS_DISCONNECTED)
        .value("IS_NOT_CONNECTED", RakNet::IS_NOT_CONNECTED);

    nb::enum_<PacketPriority>(m, "PacketPriority")
        .value("IMMEDIATE_PRIORITY", IMMEDIATE_PRIORITY)
        .value("HIGH_PRIORITY", HIGH_PRIORITY)
        .value("MEDIUM_PRIORITY", MEDIUM_PRIORITY)
        .value("LOW_PRIORITY", LOW_PRIORITY);

    nb::enum_<PacketReliability>(m, "PacketReliability")
        .value("UNRELIABLE", UNRELIABLE)
        .value("UNRELIABLE_SEQUENCED", UNRELIABLE_SEQUENCED)
        .value("RELIABLE", RELIABLE)
        .value("RELIABLE_ORDERED", RELIABLE_ORDERED)
        .value("RELIABLE_SEQUENCED", RELIABLE_SEQUENCED)
        .value("UNRELIABLE_WITH_ACK_RECEIPT", UNRELIABLE_WITH_ACK_RECEIPT)
        .value("RELIABLE_WITH_ACK_RECEIPT", RELIABLE_WITH_ACK_RECEIPT)
        .value("RELIABLE_ORDERED_WITH_ACK_RECEIPT", RELIABLE_ORDERED_WITH_ACK_RECEIPT);

    nb::enum_<RakNet::RNSPerSecondMetrics>(m, "RNSPerSecondMetrics", nb::is_arithmetic())
        .value("USER_MESSAGE_BYTES_PUSHED", RakNet::USER_MESSAGE_BYTES_PUSHED)
        .value("USER_MESSAGE_BYTES_SENT", RakNet::USER_MESSAGE_BYTES_SENT)
        .value("USER_MESSAGE_BYTES_RESENT", RakNet::USER_MESSAGE_BYTES_RESENT)
        .value("USER_MESSAGE_BYTES_RECEIVED_PROCESSED", RakNet::USER_MESSAGE_BYTES_RECEIVED_PROCESSED)
        .value("USER_MESSAGE_BYTES_RECEIVED_IGNORED", RakNet::USER_MESSAGE_BYTES_RECEIVED_IGNORED)
        .value("ACTUAL_BYTES_SENT", RakNet::ACTUAL_BYTES_SENT)
        .value("ACTUAL_BYTES_RECEIVED", RakNet::ACTUAL_BYTES_RECEIVED);

    // clang-format off
#define MESSAGE_ID(name) .value(#name, name)
    nb::enum_<DefaultMessageIDTypes>(m, "DefaultMessageIDTypes", nb::is_arithmetic())
        MESSAGE_ID(ID_CONNECTED_PING)
        MESSAGE_ID(ID_UNCONNECTED_PING)
        MESSAGE_ID(ID_UNCONNECTED_PING_OPEN_CONNECTIONS)
        MESSAGE_ID(ID_CONNECTED_PONG)
        MESSAGE_ID(ID_DETECT_LOST_CONNECTIONS)
        MESSAGE_ID(ID_OPEN_CONNECTION_REQUEST_1)
        MESSAGE_ID(ID_OPEN_CONNECTION_REPLY_1)
        MESSAGE_ID(ID_OPEN_CONNECTION_REQUEST_2)
        MESSAGE_ID(ID_OPEN_CONNECTION_REPLY_2)
        MESSAGE_ID(ID_CONNECTION_REQUEST)
        MESSAGE_ID(ID_REMOTE_SYSTEM_REQUIRES_PUBLIC_KEY)
        MESSAGE_ID(ID_OUR_SYSTEM_REQUIRES_SECURITY)
        MESSAGE_ID(ID_PUBLIC_KEY_MISMATCH)
        MESSAGE_ID(ID_OUT_OF_BAND_INTERNAL)
        MESSAGE_ID(ID_SND_RECEIPT_ACKED)
        MESSAGE_ID(ID_SND_RECEIPT_LOSS)
        MESSAGE_ID(ID_CONNECTION_REQUEST_ACCEPTED)
        MESSAGE_ID(ID_CONNECTION_ATTEMPT_FAILED)
        MESSAGE_ID(ID_ALREADY_CONNECTED)
        MESSAGE_ID(ID_NEW_INCOMING_CONNECTION)
        MESSAGE_ID(ID_NO_FREE_INCOMING_CONNECTIONS)
        MESSAGE_ID(ID_DISCONNECTION_NOTIFICATION)
        MESSAGE_ID(ID_CONNECTION_LOST)
        MESSAGE_ID(ID_CONNECTION_BANNED)
        MESSAGE_ID(ID_INVALID_PASSWORD)
        MESSAGE_ID(ID_INCOMPATIBLE_PROTOCOL_VERSION)
        MESSAGE_ID(ID_IP_RECENTLY_CONNECTED)
        MESSAGE_ID(ID_TIMESTAMP)
        MESSAGE_ID(ID_UNCONNECTED_PONG)
        MESSAGE_ID(ID_ADVERTISE_SYSTEM)
        MESSAGE_ID(ID_DOWNLOAD_PROGRESS)
        MESSAGE_ID(ID_REMOTE_DISCONNECTION_NOTIFICATION)
        MESSAGE_ID(ID_REMOTE_CONNECTION_LOST)
        MESSAGE_ID(ID_REMOTE_NEW_INCOMING_CONNECTION)
        MESSAGE_ID(ID_FILE_LIST_TRANSFER_HEADER)
        MESSAGE_ID(ID_FILE_LIST_TRANSFER_FILE)
        MESSAGE_ID(ID_FILE_LIST_REFERENCE_PUSH_ACK)
        MESSAGE_ID(ID_DDT_DOWNLOAD_REQUEST)
        MESSAGE_ID(ID_TRANSPORT_STRING)
        MESSAGE_ID(ID_REPLICA_MANAGER_CONSTRUCTION)
        MESSAGE_ID(ID_REPLICA_MANAGER_SCOPE_CHANGE)
        MESSAGE_ID(ID_REPLICA_MANAGER_SERIALIZE)
        MESSAGE_ID(ID_REPLICA_MANAGER_DOWNLOAD_STARTED)
        MESSAGE_ID(ID_REPLICA_MANAGER_DOWNLOAD_COMPLETE)
        MESSAGE_ID(ID_RAKVOICE_OPEN_CHANNEL_REQUEST)
        MESSAGE_ID(ID_RAKVOICE_OPEN_CHANNEL_REPLY)
        MESSAGE_ID(ID_RAKVOICE_CLOSE_CHANNEL)
        MESSAGE_ID(ID_RAKVOICE_DATA)
        MESSAGE_ID(ID_AUTOPATCHER_GET_CHANGELIST_SINCE_DATE)
        MESSAGE_ID(ID_AUTOPATCHER_CREATION_LIST)
        MESSAGE_ID(ID_AUTOPATCHER_DELETION_LIST)
        MESSAGE_ID(ID_AUTOPATCHER_GET_PATCH)
        MESSAGE_ID(ID_AUTOPATCHER_PATCH_LIST)
        MESSAGE_ID(ID_AUTOPATCHER_REPOSITORY_FATAL_ERROR)
        MESSAGE_ID(ID_AUTOPATCHER_CANNOT_DOWNLOAD_ORIGINAL_UNMODIFIED_FILES)
        MESSAGE_ID(ID_AUTOPATCHER_FINISHED_INTERNAL)
        MESSAGE_ID(ID_AUTOPATCHER_FINISHED)
        MESSAGE_ID(ID_AUTOPATCHER_RESTART_APPLICATION)
        MESSAGE_ID(ID_NAT_PUNCHTHROUGH_REQUEST)
        MESSAGE_ID(ID_NAT_CONNECT_AT_TIME)
        MESSAGE_ID(ID_NAT_GET_MOST_RECENT_PORT)
        MESSAGE_ID(ID_NAT_CLIENT_READY)
        MESSAGE_ID(ID_NAT_TARGET_NOT_CONNECTED)
        MESSAGE_ID(ID_NAT_TARGET_UNRESPONSIVE)
        MESSAGE_ID(ID_NAT_CONNECTION_TO_TARGET_LOST)
        MESSAGE_ID(ID_NAT_ALREADY_IN_PROGRESS)
        MESSAGE_ID(ID_NAT_PUNCHTHROUGH_FAILED)
        MESSAGE_ID(ID_NAT_PUNCHTHROUGH_SUCCEEDED)
        MESSAGE_ID(ID_READY_EVENT_SET)
        MESSAGE_ID(ID_READY_EVENT_UNSET)
        MESSAGE_ID(ID_READY_EVENT_ALL_SET)
        MESSAGE_ID(ID_READY_EVENT_QUERY)
        MESSAGE_ID(ID_LOBBY_GENERAL)
        MESSAGE_ID(ID_RPC_REMOTE_ERROR)
        MESSAGE_ID(ID_RPC_PLUGIN)
        MESSAGE_ID(ID_FILE_LIST_REFERENCE_PUSH)
        MESSAGE_ID(ID_READY_EVENT_FORCE_ALL_SET)
        MESSAGE_ID(ID_ROOMS_EXECUTE_FUNC)
        MESSAGE_ID(ID_ROOMS_LOGON_STATUS)
        MESSAGE_ID(ID_ROOMS_HANDLE_CHANGE)
        MESSAGE_ID(ID_LOBBY2_SEND_MESSAGE)
        MESSAGE_ID(ID_LOBBY2_SERVER_ERROR)
        MESSAGE_ID(ID_FCM2_NEW_HOST)
        MESSAGE_ID(ID_FCM2_REQUEST_FCMGUID)
        MESSAGE_ID(ID_FCM2_RESPOND_CONNECTION_COUNT)
        MESSAGE_ID(ID_FCM2_INFORM_FCMGUID)
        MESSAGE_ID(ID_FCM2_UPDATE_MIN_TOTAL_CONNECTION_COUNT)
        MESSAGE_ID(ID_FCM2_VERIFIED_JOIN_START)
        MESSAGE_ID(ID_FCM2_VERIFIED_JOIN_CAPABLE)
        MESSAGE_ID(ID_FCM2_VERIFIED_JOIN_FAILED)
        MESSAGE_ID(ID_FCM2_VERIFIED_JOIN_ACCEPTED)
        MESSAGE_ID(ID_FCM2_VERIFIED_JOIN_REJECTED)
        MESSAGE_ID(ID_UDP_PROXY_GENERAL)
        MESSAGE_ID(ID_SQLite3_EXEC)
        MESSAGE_ID(ID_SQLite3_UNKNOWN_DB)
        MESSAGE_ID(ID_SQLLITE_LOGGER)
        MESSAGE_ID(ID_NAT_TYPE_DETECTION_REQUEST)
        MESSAGE_ID(ID_NAT_TYPE_DETECTION_RESULT)
        MESSAGE_ID(ID_ROUTER_2_INTERNAL)
        MESSAGE_ID(ID_ROUTER_2_FORWARDING_NO_PATH)
        MESSAGE_ID(ID_ROUTER_2_FORWARDING_ESTABLISHED)
        MESSAGE_ID(ID_ROUTER_2_REROUTED)
        MESSAGE_ID(ID_TEAM_BALANCER_INTERNAL)
        MESSAGE_ID(ID_TEAM_BALANCER_REQUESTED_TEAM_FULL)
        MESSAGE_ID(ID_TEAM_BALANCER_REQUESTED_TEAM_LOCKED)
        MESSAGE_ID(ID_TEAM_BALANCER_TEAM_REQUESTED_CANCELLED)
        MESSAGE_ID(ID_TEAM_BALANCER_TEAM_ASSIGNED)
        MESSAGE_ID(ID_LIGHTSPEED_INTEGRATION)
        MESSAGE_ID(ID_XBOX_LOBBY)
        MESSAGE_ID(ID_TWO_WAY_AUTHENTICATION_INCOMING_CHALLENGE_SUCCESS)
        MESSAGE_ID(ID_TWO_WAY_AUTHENTICATION_OUTGOING_CHALLENGE_SUCCESS)
        MESSAGE_ID(ID_TWO_WAY_AUTHENTICATION_INCOMING_CHALLENGE_FAILURE)
        MESSAGE_ID(ID_TWO_WAY_AUTHENTICATION_OUTGOING_CHALLENGE_FAILURE)
        MESSAGE_ID(ID_TWO_WAY_AUTHENTICATION_OUTGOING_CHALLENGE_TIMEOUT)
        MESSAGE_ID(ID_TWO_WAY_AUTHENTICATION_NEGOTIATION)
        MESSAGE_ID(ID_CLOUD_POST_REQUEST)
        MESSAGE_ID(ID_CLOUD_RELEASE_REQUEST)
        MESSAGE_ID(ID_CLOUD_GET_REQUEST)
        MESSAGE_ID(ID_CLOUD_GET_RESPONSE)
        MESSAGE_ID(ID_CLOUD_UNSUBSCRIBE_REQUEST)
        MESSAGE_ID(ID_CLOUD_SERVER_TO_SERVER_COMMAND)
        MESSAGE_ID(ID_CLOUD_SUBSCRIPTION_NOTIFICATION)
        MESSAGE_ID(ID_LIB_VOICE)
        MESSAGE_ID(ID_RELAY_PLUGIN)
        MESSAGE_ID(ID_NAT_REQUEST_BOUND_ADDRESSES)
        MESSAGE_ID(ID_NAT_RESPOND_BOUND_ADDRESSES)
        MESSAGE_ID(ID_FCM2_UPDATE_USER_CONTEXT)
        MESSAGE_ID(ID_RESERVED_3)
        MESSAGE_ID(ID_RESERVED_4)
        MESSAGE_ID(ID_RESERVED_5)
        MESSAGE_ID(ID_RESERVED_6)
        MESSAGE_ID(ID_RESERVED_7)
        MESSAGE_ID(ID_RESERVED_8)
        MESSAGE_ID(ID_RESERVED_9)
        MESSAGE_ID(ID_USER_PACKET_ENUM);
#undef MESSAGE_ID
    // clang-format on

    nb::class_<RakNet::SystemAddress>(m, "SystemAddress")
        .def(nb::init<>())
        .def(nb::init<const char *>(), nb::arg("str"))
        .def(nb::init<const char *, unsigned short>(), nb::arg("str"), nb::arg("port"))
        .def_rw("system_index", &RakNet::SystemAddress::systemIndex)
        .def_static("size", &RakNet::SystemAddress::size)
        .def_static("to_integer", &RakNet::SystemAddress::ToInteger, nb::arg("system_address"))
        .def("get_ip_version", &RakNet::SystemAddress::GetIPVersion)
        .def("set_to_loopback", nb::overload_cast<>(&RakNet::SystemAddress::SetToLoopback))
        .def("set_to_loopback", nb::overload_cast<unsigned char>(&RakNet::SystemAddress::SetToLoopback),
             nb::arg("ip_version"))
        .def("is_loopback", &RakNet::SystemAddress::IsLoopback)
        .def("is_lan_address", &RakNet::SystemAddress::IsLANAddress)
        .def(
            "to_string",
            [](const RakNet::SystemAddress &self, bool write_port, const std::string &port_delineator) {
                return std::string(self.ToString(write_port, delineator(port_delineator)));
            },
            nb::arg("write_port") = true, nb::arg("port_delineator") = "|")
        .def(
            "from_string",
            [](RakNet::SystemAddress &self, const char *str, const std::string &port_delineator, int ip_version) {
                return self.FromString(str, delineator(port_delineator), ip_version);
            },
            nb::arg("str"), nb::arg("port_delineator") = "|", nb::arg("ip_version") = 0)
        .def("from_string_explicit_port", &RakNet::SystemAddress::FromStringExplicitPort, nb::arg("str"),
             nb::arg("port"), nb::arg("ip_version") = 0)
        .def("copy_port", nb::overload_cast<const RakNet::SystemAddress &>(&RakNet::SystemAddress::CopyPort),
             nb::arg("right"))
        .def("equals_excluding_port", &RakNet::SystemAddress::EqualsExcludingPort, nb::arg("right"))
        .def("get_port", &RakNet::SystemAddress::GetPort)
        .def("get_port_network_order", &RakNet::SystemAddress::GetPortNetworkOrder)
        .def("set_port_host_order", &RakNet::SystemAddress::SetPortHostOrder, nb::arg("port"))
        .def("set_port_network_order", nb::overload_cast<unsigned short>(&RakNet::SystemAddress::SetPortNetworkOrder),
             nb::arg("port"))
        .def("is_lan_address", &RakNet::SystemAddress::IsLANAddress)
        .def(nb::self == nb::self)
        .def(nb::self != nb::self)
        .def(nb::self < nb::self)
        .def(nb::self > nb::self)
        .def("__hash__", [](const RakNet::SystemAddress &self) { return RakNet::SystemAddress::ToInteger(self); })
        .def("__str__", [](const RakNet::SystemAddress &self) { return std::string(self.ToString(true, '|')); })
        .def("__repr__", [](const RakNet::SystemAddress &self) {
            return "SystemAddress('" + std::string(self.ToString(true, '|')) + "')";
        });

    nb::class_<RakNet::RakNetGUID>(m, "RakNetGUID")
        .def(nb::init<>())
        .def(nb::init<uint64_t>(), nb::arg("g"))
        .def_rw("g", &RakNet::RakNetGUID::g)
        .def_rw("system_index", &RakNet::RakNetGUID::systemIndex)
        .def_static("size", &RakNet::RakNetGUID::size)
        .def_static("to_uint32", &RakNet::RakNetGUID::ToUint32, nb::arg("guid"))
        .def("to_string", [](const RakNet::RakNetGUID &self) { return std::string(self.ToString()); })
        .def("from_string", &RakNet::RakNetGUID::FromString, nb::arg("source"))
        .def(nb::self == nb::self)
        .def(nb::self != nb::self)
        .def(nb::self < nb::self)
        .def(nb::self > nb::self)
        .def("__hash__", [](const RakNet::RakNetGUID &self) { return self.g; })
        .def("__str__", [](const RakNet::RakNetGUID &self) { return std::string(self.ToString()); })
        .def("__repr__", [](const RakNet::RakNetGUID &self) { return "RakNetGUID(" + std::to_string(self.g) + ")"; });

    nb::class_<RakNet::AddressOrGUID>(m, "AddressOrGUID")
        .def(nb::init<>())
        .def(nb::init<const RakNet::SystemAddress &>(), nb::arg("system_address"))
        .def(nb::init<const RakNet::RakNetGUID &>(), nb::arg("guid"))
        .def_rw("rak_net_guid", &RakNet::AddressOrGUID::rakNetGuid)
        .def_rw("system_address", &RakNet::AddressOrGUID::systemAddress)
        .def("get_system_index", &RakNet::AddressOrGUID::GetSystemIndex)
        .def("is_undefined", &RakNet::AddressOrGUID::IsUndefined)
        .def("set_undefined", &RakNet::AddressOrGUID::SetUndefined)
        .def_static("to_integer", &RakNet::AddressOrGUID::ToInteger, nb::arg("address_or_guid"))
        .def(
            "to_string",
            [](const RakNet::AddressOrGUID &self, bool write_port) { return std::string(self.ToString(write_port)); },
            nb::arg("write_port") = true)
        .def("__str__", [](const RakNet::AddressOrGUID &self) { return std::string(self.ToString(true)); });
    nb::implicitly_convertible<RakNet::SystemAddress, RakNet::AddressOrGUID>();
    nb::implicitly_convertible<RakNet::RakNetGUID, RakNet::AddressOrGUID>();

    m.attr("UNASSIGNED_SYSTEM_ADDRESS") = RakNet::UNASSIGNED_SYSTEM_ADDRESS;
    m.attr("UNASSIGNED_RAKNET_GUID") = RakNet::UNASSIGNED_RAKNET_GUID;

    nb::class_<RakNet::SocketDescriptor>(m, "SocketDescriptor")
        .def(nb::init<>())
        .def(nb::init<unsigned short, const char *>(), nb::arg("port"), nb::arg("host_address"))
        .def_rw("port", &RakNet::SocketDescriptor::port)
        .def_prop_rw(
            "host_address", [](const RakNet::SocketDescriptor &self) { return std::string(self.hostAddress); },
            [](RakNet::SocketDescriptor &self, const std::string &value) {
                std::strncpy(self.hostAddress, value.c_str(), sizeof(self.hostAddress) - 1);
                self.hostAddress[sizeof(self.hostAddress) - 1] = '\0';
            })
        .def_rw("socket_family", &RakNet::SocketDescriptor::socketFamily)
        .def_rw("blocking_socket", &RakNet::SocketDescriptor::blockingSocket)
        .def_rw("extra_socket_options", &RakNet::SocketDescriptor::extraSocketOptions);

    nb::class_<Packet>(m, "Packet")
        .def_ro("system_address", &Packet::system_address)
        .def_ro("guid", &Packet::guid)
        .def_ro("length", &Packet::length)
        .def_ro("bit_size", &Packet::bit_size)
        .def_ro("data", &Packet::data)
        .def_ro("was_generated_locally", &Packet::was_generated_locally);

    nb::class_<RakNet::RakNetStatistics>(m, "RakNetStatistics")
        .def_prop_ro("value_over_last_second",
                     [](const RakNet::RakNetStatistics &self) {
                         return std::vector<uint64_t>(std::begin(self.valueOverLastSecond),
                                                      std::end(self.valueOverLastSecond));
                     })
        .def_prop_ro("running_total",
                     [](const RakNet::RakNetStatistics &self) {
                         return std::vector<uint64_t>(std::begin(self.runningTotal), std::end(self.runningTotal));
                     })
        .def_ro("connection_start_time", &RakNet::RakNetStatistics::connectionStartTime)
        .def_ro("is_limited_by_congestion_control", &RakNet::RakNetStatistics::isLimitedByCongestionControl)
        .def_ro("bps_limit_by_congestion_control", &RakNet::RakNetStatistics::BPSLimitByCongestionControl)
        .def_ro("is_limited_by_outgoing_bandwidth_limit", &RakNet::RakNetStatistics::isLimitedByOutgoingBandwidthLimit)
        .def_ro("bps_limit_by_outgoing_bandwidth_limit", &RakNet::RakNetStatistics::BPSLimitByOutgoingBandwidthLimit)
        .def_prop_ro("message_in_send_buffer",
                     [](const RakNet::RakNetStatistics &self) {
                         return std::vector<unsigned int>(std::begin(self.messageInSendBuffer),
                                                          std::end(self.messageInSendBuffer));
                     })
        .def_prop_ro("bytes_in_send_buffer",
                     [](const RakNet::RakNetStatistics &self) {
                         return std::vector<double>(std::begin(self.bytesInSendBuffer),
                                                    std::end(self.bytesInSendBuffer));
                     })
        .def_ro("messages_in_resend_buffer", &RakNet::RakNetStatistics::messagesInResendBuffer)
        .def_ro("bytes_in_resend_buffer", &RakNet::RakNetStatistics::bytesInResendBuffer)
        .def_ro("packetloss_last_second", &RakNet::RakNetStatistics::packetlossLastSecond)
        .def_ro("packetloss_total", &RakNet::RakNetStatistics::packetlossTotal)
        .def(
            "to_string",
            [](RakNet::RakNetStatistics &self, int verbosity_level) {
                char buffer[4096];
                RakNet::StatisticsToString(&self, buffer, verbosity_level);
                return std::string(buffer);
            },
            nb::arg("verbosity_level") = 0)
        .def("__str__", [](RakNet::RakNetStatistics &self) {
            char buffer[4096];
            RakNet::StatisticsToString(&self, buffer, 0);
            return std::string(buffer);
        });

    nb::class_<RakPeer>(m, "RakPeer")
        .def(nb::init<>())
        .def(
            "startup",
            [](RakPeer &self, unsigned int max_connections, std::vector<RakNet::SocketDescriptor> socket_descriptors,
               int thread_priority) {
                nb::gil_scoped_release release;
                return self->Startup(max_connections, socket_descriptors.data(),
                                     static_cast<unsigned>(socket_descriptors.size()), thread_priority);
            },
            nb::arg("max_connections"), nb::arg("socket_descriptors"), nb::arg("thread_priority") = -99999)
        .def(
            "initialize_security",
            [](RakPeer &self, std::optional<nb::bytes> public_key, std::optional<nb::bytes> private_key,
               bool require_client_key) {
                return self->InitializeSecurity(public_key ? public_key->c_str() : nullptr,
                                                private_key ? private_key->c_str() : nullptr, require_client_key);
            },
            nb::arg("public_key").none(), nb::arg("private_key").none(), nb::arg("require_client_key") = false)
        .def("disable_security", [](RakPeer &self) { self->DisableSecurity(); })
        .def(
            "add_to_security_exception_list",
            [](RakPeer &self, const char *ip) { self->AddToSecurityExceptionList(ip); }, nb::arg("ip"))
        .def(
            "remove_from_security_exception_list",
            [](RakPeer &self, const char *ip) { self->RemoveFromSecurityExceptionList(ip); }, nb::arg("ip"))
        .def(
            "is_in_security_exception_list",
            [](RakPeer &self, const char *ip) { return self->IsInSecurityExceptionList(ip); }, nb::arg("ip"))
        .def(
            "set_maximum_incoming_connections",
            [](RakPeer &self, unsigned short number_allowed) { self->SetMaximumIncomingConnections(number_allowed); },
            nb::arg("number_allowed"))
        .def("get_maximum_incoming_connections",
             [](const RakPeer &self) { return self->GetMaximumIncomingConnections(); })
        .def("number_of_connections", [](const RakPeer &self) { return self->NumberOfConnections(); })
        .def(
            "set_incoming_password",
            [](RakPeer &self, const nb::bytes &password) {
                self->SetIncomingPassword(password.c_str(), static_cast<int>(password.size()));
            },
            nb::arg("password"))
        .def("get_incoming_password",
             [](RakPeer &self) {
                 char password[256];
                 int length = sizeof(password);
                 self->GetIncomingPassword(password, &length);
                 return nb::bytes(password, length);
             })
        .def(
            "connect",
            [](RakPeer &self, const char *host, unsigned short remote_port, const nb::bytes &password,
               unsigned connection_socket_index, unsigned send_connection_attempt_count,
               unsigned time_between_send_connection_attempts_ms, RakNet::TimeMS timeout_time) {
                const char *password_data = password.c_str();
                int password_length = static_cast<int>(password.size());
                nb::gil_scoped_release release;
                return self->Connect(host, remote_port, password_data, password_length, nullptr,
                                     connection_socket_index, send_connection_attempt_count,
                                     time_between_send_connection_attempts_ms, timeout_time);
            },
            nb::arg("host"), nb::arg("remote_port"), nb::arg("password") = nb::bytes("", 0),
            nb::arg("connection_socket_index") = 0, nb::arg("send_connection_attempt_count") = 12,
            nb::arg("time_between_send_connection_attempts_ms") = 500, nb::arg("timeout_time") = 0)
        .def(
            "shutdown",
            [](RakPeer &self, unsigned int block_duration, int ordering_channel,
               PacketPriority disconnection_notification_priority) {
                nb::gil_scoped_release release;
                self->Shutdown(block_duration, static_cast<unsigned char>(ordering_channel),
                               disconnection_notification_priority);
            },
            nb::arg("block_duration"), nb::arg("ordering_channel") = 0,
            nb::arg("disconnection_notification_priority") = LOW_PRIORITY)
        .def("is_active", [](const RakPeer &self) { return self->IsActive(); })
        .def("get_connection_list",
             [](RakPeer &self) {
                 unsigned short count = static_cast<unsigned short>(self->GetMaximumNumberOfPeers());
                 std::vector<RakNet::SystemAddress> addresses(count);
                 if (!self->GetConnectionList(addresses.data(), &count)) {
                     return std::vector<RakNet::SystemAddress>{};
                 }
                 addresses.resize(count);
                 return addresses;
             })
        .def("get_next_send_receipt", [](RakPeer &self) { return self->GetNextSendReceipt(); })
        .def("increment_next_send_receipt", [](RakPeer &self) { return self->IncrementNextSendReceipt(); })
        .def(
            "send",
            [](RakPeer &self, const nb::bytes &data, PacketPriority priority, PacketReliability reliability,
               int ordering_channel, const RakNet::AddressOrGUID &system_identifier, bool broadcast,
               uint32_t force_receipt_number) {
                const char *buffer = data.c_str();
                int length = static_cast<int>(data.size());
                nb::gil_scoped_release release;
                return self->Send(buffer, length, priority, reliability, static_cast<char>(ordering_channel),
                                  system_identifier, broadcast, force_receipt_number);
            },
            nb::arg("data"), nb::arg("priority"), nb::arg("reliability"), nb::arg("ordering_channel"),
            nb::arg("system_identifier"), nb::arg("broadcast"), nb::arg("force_receipt_number") = 0)
        .def(
            "send_loopback",
            [](RakPeer &self, const nb::bytes &data) {
                self->SendLoopback(data.c_str(), static_cast<int>(data.size()));
            },
            nb::arg("data"))
        .def(
            "send_list",
            [](RakPeer &self, const std::vector<nb::bytes> &data, PacketPriority priority,
               PacketReliability reliability, int ordering_channel, const RakNet::AddressOrGUID &system_identifier,
               bool broadcast, uint32_t force_receipt_number) {
                std::vector<const char *> pointers;
                std::vector<int> lengths;
                pointers.reserve(data.size());
                lengths.reserve(data.size());
                for (const auto &item : data) {
                    pointers.push_back(item.c_str());
                    lengths.push_back(static_cast<int>(item.size()));
                }
                nb::gil_scoped_release release;
                return self->SendList(pointers.data(), lengths.data(), static_cast<int>(pointers.size()), priority,
                                      reliability, static_cast<char>(ordering_channel), system_identifier, broadcast,
                                      force_receipt_number);
            },
            nb::arg("data"), nb::arg("priority"), nb::arg("reliability"), nb::arg("ordering_channel"),
            nb::arg("system_identifier"), nb::arg("broadcast"), nb::arg("force_receipt_number") = 0)
        .def("receive",
             [](RakPeer &self) -> std::optional<Packet> {
                 RakNet::Packet *packet;
                 {
                     nb::gil_scoped_release release;
                     packet = self->Receive();
                 }
                 if (!packet) {
                     return std::nullopt;
                 }
                 Packet result{packet->systemAddress,
                               packet->guid,
                               packet->length,
                               packet->bitSize,
                               nb::bytes(packet->data, packet->length),
                               packet->wasGeneratedLocally};
                 self->DeallocatePacket(packet);
                 return result;
             })
        .def("get_maximum_number_of_peers", [](const RakPeer &self) { return self->GetMaximumNumberOfPeers(); })
        .def(
            "close_connection",
            [](RakPeer &self, const RakNet::AddressOrGUID &target, bool send_disconnection_notification,
               int ordering_channel, PacketPriority disconnection_notification_priority) {
                self->CloseConnection(target, send_disconnection_notification,
                                      static_cast<unsigned char>(ordering_channel),
                                      disconnection_notification_priority);
            },
            nb::arg("target"), nb::arg("send_disconnection_notification"), nb::arg("ordering_channel") = 0,
            nb::arg("disconnection_notification_priority") = LOW_PRIORITY)
        .def(
            "get_connection_state",
            [](RakPeer &self, const RakNet::AddressOrGUID &system_identifier) {
                return self->GetConnectionState(system_identifier);
            },
            nb::arg("system_identifier"))
        .def(
            "cancel_connection_attempt",
            [](RakPeer &self, const RakNet::SystemAddress &target) { self->CancelConnectionAttempt(target); },
            nb::arg("target"))
        .def(
            "get_index_from_system_address",
            [](const RakPeer &self, const RakNet::SystemAddress &system_address) {
                return self->GetIndexFromSystemAddress(system_address);
            },
            nb::arg("system_address"))
        .def(
            "get_system_address_from_index",
            [](RakPeer &self, unsigned int index) { return self->GetSystemAddressFromIndex(index); }, nb::arg("index"))
        .def(
            "get_guid_from_index", [](RakPeer &self, unsigned int index) { return self->GetGUIDFromIndex(index); },
            nb::arg("index"))
        .def("get_system_list",
             [](const RakPeer &self) {
                 DataStructures::List<RakNet::SystemAddress> addresses;
                 DataStructures::List<RakNet::RakNetGUID> guids;
                 self->GetSystemList(addresses, guids);
                 std::vector<RakNet::SystemAddress> address_list;
                 std::vector<RakNet::RakNetGUID> guid_list;
                 for (unsigned int i = 0; i < addresses.Size(); i++) {
                     address_list.push_back(addresses[i]);
                 }
                 for (unsigned int i = 0; i < guids.Size(); i++) {
                     guid_list.push_back(guids[i]);
                 }
                 return std::make_pair(std::move(address_list), std::move(guid_list));
             })
        .def(
            "add_to_ban_list",
            [](RakPeer &self, const char *ip, RakNet::TimeMS milliseconds) { self->AddToBanList(ip, milliseconds); },
            nb::arg("ip"), nb::arg("milliseconds") = 0)
        .def(
            "remove_from_ban_list", [](RakPeer &self, const char *ip) { self->RemoveFromBanList(ip); }, nb::arg("ip"))
        .def("clear_ban_list", [](RakPeer &self) { self->ClearBanList(); })
        .def(
            "is_banned", [](RakPeer &self, const char *ip) { return self->IsBanned(ip); }, nb::arg("ip"))
        .def(
            "set_limit_ip_connection_frequency",
            [](RakPeer &self, bool limit) { self->SetLimitIPConnectionFrequency(limit); }, nb::arg("limit"))
        .def(
            "ping", [](RakPeer &self, const RakNet::SystemAddress &target) { self->Ping(target); }, nb::arg("target"))
        .def(
            "ping",
            [](RakPeer &self, const char *host, unsigned short remote_port, bool only_reply_on_accepting_connections,
               unsigned connection_socket_index) {
                nb::gil_scoped_release release;
                return self->Ping(host, remote_port, only_reply_on_accepting_connections, connection_socket_index);
            },
            nb::arg("host"), nb::arg("remote_port"), nb::arg("only_reply_on_accepting_connections"),
            nb::arg("connection_socket_index") = 0)
        .def(
            "get_average_ping",
            [](RakPeer &self, const RakNet::AddressOrGUID &system_identifier) {
                return self->GetAveragePing(system_identifier);
            },
            nb::arg("system_identifier"))
        .def(
            "get_last_ping",
            [](const RakPeer &self, const RakNet::AddressOrGUID &system_identifier) {
                return self->GetLastPing(system_identifier);
            },
            nb::arg("system_identifier"))
        .def(
            "get_lowest_ping",
            [](const RakPeer &self, const RakNet::AddressOrGUID &system_identifier) {
                return self->GetLowestPing(system_identifier);
            },
            nb::arg("system_identifier"))
        .def(
            "set_occasional_ping", [](RakPeer &self, bool do_ping) { self->SetOccasionalPing(do_ping); },
            nb::arg("do_ping"))
        .def(
            "get_clock_differential",
            [](RakPeer &self, const RakNet::AddressOrGUID &system_identifier) {
                return self->GetClockDifferential(system_identifier);
            },
            nb::arg("system_identifier"))
        .def(
            "set_offline_ping_response",
            [](RakPeer &self, const nb::bytes &data) {
                self->SetOfflinePingResponse(data.c_str(), static_cast<unsigned int>(data.size()));
            },
            nb::arg("data"))
        .def("get_offline_ping_response",
             [](RakPeer &self) {
                 char *data;
                 unsigned int length;
                 self->GetOfflinePingResponse(&data, &length);
                 return nb::bytes(data, length);
             })
        .def(
            "get_internal_id",
            [](const RakPeer &self, const RakNet::SystemAddress &system_address, int index) {
                return self->GetInternalID(system_address, index);
            },
            nb::arg("system_address") = RakNet::UNASSIGNED_SYSTEM_ADDRESS, nb::arg("index") = 0)
        .def(
            "set_internal_id",
            [](RakPeer &self, const RakNet::SystemAddress &system_address, int index) {
                self->SetInternalID(system_address, index);
            },
            nb::arg("system_address"), nb::arg("index") = 0)
        .def(
            "get_external_id",
            [](const RakPeer &self, const RakNet::SystemAddress &target) { return self->GetExternalID(target); },
            nb::arg("target"))
        .def("get_my_guid", [](const RakPeer &self) { return self->GetMyGUID(); })
        .def(
            "get_my_bound_address",
            [](RakPeer &self, int socket_index) { return self->GetMyBoundAddress(socket_index); },
            nb::arg("socket_index") = 0)
        .def(
            "get_guid_from_system_address",
            [](const RakPeer &self, const RakNet::SystemAddress &input) {
                return self->GetGuidFromSystemAddress(input);
            },
            nb::arg("input"))
        .def(
            "get_system_address_from_guid",
            [](const RakPeer &self, const RakNet::RakNetGUID &input) { return self->GetSystemAddressFromGuid(input); },
            nb::arg("input"))
        .def(
            "get_client_public_key_from_system_address",
            [](const RakPeer &self, const RakNet::SystemAddress &input) -> std::optional<nb::bytes> {
                char key[64];
                if (!self->GetClientPublicKeyFromSystemAddress(input, key)) {
                    return std::nullopt;
                }
                return nb::bytes(key, sizeof(key));
            },
            nb::arg("input"))
        .def(
            "set_timeout_time",
            [](RakPeer &self, RakNet::TimeMS time_ms, const RakNet::SystemAddress &target) {
                self->SetTimeoutTime(time_ms, target);
            },
            nb::arg("time_ms"), nb::arg("target"))
        .def(
            "get_timeout_time",
            [](RakPeer &self, const RakNet::SystemAddress &target) { return self->GetTimeoutTime(target); },
            nb::arg("target"))
        .def(
            "get_mtu_size",
            [](const RakPeer &self, const RakNet::SystemAddress &target) { return self->GetMTUSize(target); },
            nb::arg("target"))
        .def("get_number_of_addresses", [](RakPeer &self) { return self->GetNumberOfAddresses(); })
        .def(
            "get_local_ip", [](RakPeer &self, unsigned int index) { return std::string(self->GetLocalIP(index)); },
            nb::arg("index"))
        .def(
            "is_local_ip", [](RakPeer &self, const char *ip) { return self->IsLocalIP(ip); }, nb::arg("ip"))
        .def(
            "allow_connection_response_ip_migration",
            [](RakPeer &self, bool allow) { self->AllowConnectionResponseIPMigration(allow); }, nb::arg("allow"))
        .def(
            "advertise_system",
            [](RakPeer &self, const char *host, unsigned short remote_port, const nb::bytes &data,
               unsigned connection_socket_index) {
                const char *buffer = data.c_str();
                int length = static_cast<int>(data.size());
                nb::gil_scoped_release release;
                return self->AdvertiseSystem(host, remote_port, buffer, length, connection_socket_index);
            },
            nb::arg("host"), nb::arg("remote_port"), nb::arg("data") = nb::bytes("", 0),
            nb::arg("connection_socket_index") = 0)
        .def(
            "set_split_message_progress_interval",
            [](RakPeer &self, int interval) { self->SetSplitMessageProgressInterval(interval); }, nb::arg("interval"))
        .def("get_split_message_progress_interval",
             [](const RakPeer &self) { return self->GetSplitMessageProgressInterval(); })
        .def(
            "set_unreliable_timeout",
            [](RakPeer &self, RakNet::TimeMS timeout_ms) { self->SetUnreliableTimeout(timeout_ms); },
            nb::arg("timeout_ms"))
        .def(
            "send_ttl",
            [](RakPeer &self, const char *host, unsigned short remote_port, int ttl, unsigned connection_socket_index) {
                nb::gil_scoped_release release;
                self->SendTTL(host, remote_port, ttl, connection_socket_index);
            },
            nb::arg("host"), nb::arg("remote_port"), nb::arg("ttl"), nb::arg("connection_socket_index") = 0)
        .def(
            "change_system_address",
            [](RakPeer &self, const RakNet::RakNetGUID &guid, const RakNet::SystemAddress &system_address) {
                self->ChangeSystemAddress(guid, system_address);
            },
            nb::arg("guid"), nb::arg("system_address"))
        .def(
            "apply_network_simulator",
            [](RakPeer &self, float packetloss, unsigned short min_extra_ping, unsigned short extra_ping_variance) {
                self->ApplyNetworkSimulator(packetloss, min_extra_ping, extra_ping_variance);
            },
            nb::arg("packetloss"), nb::arg("min_extra_ping"), nb::arg("extra_ping_variance"))
        .def(
            "set_per_connection_outgoing_bandwidth_limit",
            [](RakPeer &self, unsigned max_bits_per_second) {
                self->SetPerConnectionOutgoingBandwidthLimit(max_bits_per_second);
            },
            nb::arg("max_bits_per_second"))
        .def("is_network_simulator_active", [](RakPeer &self) { return self->IsNetworkSimulatorActive(); })
        .def(
            "get_statistics",
            [](RakPeer &self, const RakNet::SystemAddress &system_address) -> std::optional<RakNet::RakNetStatistics> {
                RakNet::RakNetStatistics statistics;
                if (!self->GetStatistics(system_address, &statistics)) {
                    return std::nullopt;
                }
                return statistics;
            },
            nb::arg("system_address"))
        .def(
            "get_statistics",
            [](RakPeer &self, unsigned int index) -> std::optional<RakNet::RakNetStatistics> {
                RakNet::RakNetStatistics statistics;
                if (!self->GetStatistics(index, &statistics)) {
                    return std::nullopt;
                }
                return statistics;
            },
            nb::arg("index"))
        .def("get_statistics_list",
             [](const RakPeer &self) {
                 DataStructures::List<RakNet::SystemAddress> addresses;
                 DataStructures::List<RakNet::RakNetGUID> guids;
                 DataStructures::List<RakNet::RakNetStatistics> statistics;
                 self->GetStatisticsList(addresses, guids, statistics);
                 std::vector<std::tuple<RakNet::SystemAddress, RakNet::RakNetGUID, RakNet::RakNetStatistics>> result;
                 for (unsigned int i = 0; i < addresses.Size(); i++) {
                     result.emplace_back(addresses[i], guids[i], statistics[i]);
                 }
                 return result;
             })
        .def("get_receive_buffer_size", [](RakPeer &self) { return self->GetReceiveBufferSize(); })
        .def(
            "send_out_of_band",
            [](RakPeer &self, const char *host, unsigned short remote_port, const nb::bytes &data,
               unsigned connection_socket_index) {
                const char *buffer = data.c_str();
                auto length = static_cast<RakNet::BitSize_t>(data.size());
                nb::gil_scoped_release release;
                return self->SendOutOfBand(host, remote_port, buffer, length, connection_socket_index);
            },
            nb::arg("host"), nb::arg("remote_port"), nb::arg("data") = nb::bytes("", 0),
            nb::arg("connection_socket_index") = 0);

    m.def("get_time", &RakNet::GetTime);
    m.def("get_time_ms", &RakNet::GetTimeMS);
    m.def("get_time_us", &RakNet::GetTimeUS);
}
