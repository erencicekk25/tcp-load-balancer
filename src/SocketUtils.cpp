#include "SocketUtils.hpp"

#include <algorithm>
#include <chrono>
#include <climits>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <thread>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace tlb::net {

namespace {

#ifdef _WIN32
using NativeSocket = SOCKET;
using NativePollFd = WSAPOLLFD;
constexpr int kSendFlags = 0;
constexpr int kShutdownWrite = SD_SEND;
#else
using NativeSocket = int;
using NativePollFd = pollfd;
#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif
constexpr int kShutdownWrite = SHUT_WR;
#endif

NativeSocket native(SocketHandle socket) {
    return static_cast<NativeSocket>(socket);
}

SocketHandle wrap(NativeSocket socket) {
    return static_cast<SocketHandle>(socket);
}

bool isWouldBlock(int code) {
#ifdef _WIN32
    return code == WSAEWOULDBLOCK || code == WSAEINTR;
#else
    return code == EAGAIN || code == EWOULDBLOCK || code == EINTR;
#endif
}

bool isConnectInProgress(int code) {
#ifdef _WIN32
    return code == WSAEWOULDBLOCK || code == WSAEINPROGRESS;
#else
    return code == EINPROGRESS || code == EINTR;
#endif
}

struct AddrInfoDeleter {
    void operator()(addrinfo* info) const noexcept {
        if (info != nullptr) {
            freeaddrinfo(info);
        }
    }
};

using AddrInfoPtr = std::unique_ptr<addrinfo, AddrInfoDeleter>;

AddrInfoPtr resolve(const std::string& host, std::uint16_t port, bool passive) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = passive ? AI_PASSIVE : 0;

    const char* node = host.empty() ? nullptr : host.c_str();
    const std::string service = std::to_string(port);

    addrinfo* result = nullptr;
    const int rc = getaddrinfo(node, service.c_str(), &hints, &result);
    if (rc != 0) {
        throw std::runtime_error("cannot resolve '" + host + ":" + service + "': " + gai_strerror(rc));
    }
    return AddrInfoPtr(result);
}

}  // namespace

// ---------------------------------------------------------------------------
// NetworkContext
// ---------------------------------------------------------------------------

NetworkContext::NetworkContext() {
#ifdef _WIN32
    WSADATA data{};
    const int rc = WSAStartup(MAKEWORD(2, 2), &data);
    if (rc != 0) {
        throw std::runtime_error("WSAStartup failed: " + describeError(rc));
    }
#else
    // Writing to a socket whose peer is gone must not kill the process.
    std::signal(SIGPIPE, SIG_IGN);
#endif
}

NetworkContext::~NetworkContext() {
#ifdef _WIN32
    WSACleanup();
#endif
}

// ---------------------------------------------------------------------------
// Socket lifecycle
// ---------------------------------------------------------------------------

SocketHandle createListener(const std::string& host, std::uint16_t port, int backlog) {
    const AddrInfoPtr address = resolve(host, port, true);

    const NativeSocket socket = ::socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (wrap(socket) == kInvalidSocket) {
        throw std::runtime_error("socket() failed: " + describeError(lastError()));
    }

    int enable = 1;
#ifdef _WIN32
    // SO_REUSEADDR on Windows allows port hijacking; exclusive use is the safe equivalent.
    setsockopt(socket, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, reinterpret_cast<const char*>(&enable), sizeof(enable));
#else
    setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));
#endif

    if (::bind(socket, address->ai_addr, static_cast<socklen_t>(address->ai_addrlen)) != 0) {
        const int code = lastError();
        closeSocket(wrap(socket));
        throw std::runtime_error("bind() on port " + std::to_string(port) + " failed: " + describeError(code));
    }

    if (::listen(socket, backlog) != 0) {
        const int code = lastError();
        closeSocket(wrap(socket));
        throw std::runtime_error("listen() failed: " + describeError(code));
    }

    return wrap(socket);
}

SocketHandle acceptClient(SocketHandle listener, std::string* peerAddress) {
    sockaddr_in address{};
    socklen_t length = sizeof(address);

    const NativeSocket client = ::accept(native(listener), reinterpret_cast<sockaddr*>(&address), &length);
    if (wrap(client) == kInvalidSocket) {
        return kInvalidSocket;
    }

    if (peerAddress != nullptr) {
        char ip[INET_ADDRSTRLEN] = {};
        inet_ntop(AF_INET, &address.sin_addr, ip, sizeof(ip));
        *peerAddress = std::string(ip) + ":" + std::to_string(ntohs(address.sin_port));
    }
    return wrap(client);
}

SocketHandle connectTo(const std::string& host, std::uint16_t port, bool nonBlocking, bool& inProgress) {
    inProgress = false;

    AddrInfoPtr address;
    try {
        address = resolve(host, port, false);
    } catch (const std::exception&) {
        return kInvalidSocket;
    }

    const NativeSocket socket = ::socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (wrap(socket) == kInvalidSocket) {
        return kInvalidSocket;
    }

    if (nonBlocking && !setNonBlocking(wrap(socket))) {
        closeSocket(wrap(socket));
        return kInvalidSocket;
    }

    if (::connect(socket, address->ai_addr, static_cast<socklen_t>(address->ai_addrlen)) == 0) {
        return wrap(socket);
    }

    const int code = lastError();
    if (nonBlocking && isConnectInProgress(code)) {
        inProgress = true;
        return wrap(socket);
    }

    closeSocket(wrap(socket));
    return kInvalidSocket;
}

bool setNonBlocking(SocketHandle socket) {
#ifdef _WIN32
    u_long mode = 1;
    return ioctlsocket(native(socket), FIONBIO, &mode) == 0;
#else
    const int flags = fcntl(socket, F_GETFL, 0);
    return flags >= 0 && fcntl(socket, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
}

void setNoDelay(SocketHandle socket) {
    int enable = 1;
    setsockopt(native(socket), IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&enable), sizeof(enable));
}

int pendingError(SocketHandle socket) {
    int value = 0;
    socklen_t length = sizeof(value);
    if (getsockopt(native(socket), SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&value), &length) != 0) {
        return lastError();
    }
    return value;
}

// ---------------------------------------------------------------------------
// I/O
// ---------------------------------------------------------------------------

IoResult receive(SocketHandle socket, char* data, std::size_t size) {
#ifdef _WIN32
    const int count = ::recv(native(socket), data, static_cast<int>(std::min<std::size_t>(size, INT_MAX)), 0);
#else
    const ssize_t count = ::recv(socket, data, size, 0);
#endif
    if (count > 0) {
        return {IoStatus::Ok, static_cast<std::size_t>(count)};
    }
    if (count == 0) {
        return {IoStatus::Closed, 0};
    }
    return {isWouldBlock(lastError()) ? IoStatus::WouldBlock : IoStatus::Error, 0};
}

IoResult transmit(SocketHandle socket, const char* data, std::size_t size) {
#ifdef _WIN32
    const int count = ::send(native(socket), data, static_cast<int>(std::min<std::size_t>(size, INT_MAX)), kSendFlags);
#else
    const ssize_t count = ::send(socket, data, size, kSendFlags);
#endif
    if (count >= 0) {
        return {IoStatus::Ok, static_cast<std::size_t>(count)};
    }
    return {isWouldBlock(lastError()) ? IoStatus::WouldBlock : IoStatus::Error, 0};
}

void shutdownWrite(SocketHandle socket) {
    ::shutdown(native(socket), kShutdownWrite);
}

void closeSocket(SocketHandle socket) {
    if (socket == kInvalidSocket) {
        return;
    }
#ifdef _WIN32
    ::closesocket(native(socket));
#else
    ::close(socket);
#endif
}

int poll(std::vector<PollItem>& items, int timeoutMs) {
    if (items.empty()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(timeoutMs));
        return 0;
    }

    std::vector<NativePollFd> descriptors(items.size());
    for (std::size_t i = 0; i < items.size(); ++i) {
        descriptors[i].fd = native(items[i].socket);
        descriptors[i].events = static_cast<short>((items[i].wantRead ? POLLIN : 0) | (items[i].wantWrite ? POLLOUT : 0));
        descriptors[i].revents = 0;
    }

#ifdef _WIN32
    const int rc = WSAPoll(descriptors.data(), static_cast<ULONG>(descriptors.size()), timeoutMs);
#else
    const int rc = ::poll(descriptors.data(), static_cast<nfds_t>(descriptors.size()), timeoutMs);
#endif

    if (rc < 0) {
        return isWouldBlock(lastError()) ? 0 : -1;
    }

    for (std::size_t i = 0; i < items.size(); ++i) {
        const short events = descriptors[i].revents;
        // HUP/ERR are reported as readable/writable so that the next recv/send
        // call surfaces the real condition (EOF or a concrete error code).
        items[i].readable = (events & (POLLIN | POLLHUP | POLLERR)) != 0;
        items[i].writable = (events & (POLLOUT | POLLERR)) != 0;
        items[i].failed = (events & (POLLERR | POLLNVAL)) != 0;
    }
    return rc;
}

std::uint16_t localPort(SocketHandle socket) {
    sockaddr_in address{};
    socklen_t length = sizeof(address);
    if (getsockname(native(socket), reinterpret_cast<sockaddr*>(&address), &length) != 0) {
        return 0;
    }
    return ntohs(address.sin_port);
}

int lastError() {
#ifdef _WIN32
    return WSAGetLastError();
#else
    return errno;
#endif
}

std::string describeError(int code) {
    return std::system_category().message(code) + " (code " + std::to_string(code) + ")";
}

}  // namespace tlb::net
