#pragma once

// Thin, cross-platform wrapper around BSD sockets / Winsock.
// Keeps platform headers out of the public API so that the rest of the
// project can stay free of #ifdef noise.

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace tlb::net {

#ifdef _WIN32
using SocketHandle = std::uintptr_t;
#else
using SocketHandle = int;
#endif

inline const SocketHandle kInvalidSocket = static_cast<SocketHandle>(-1);

// RAII guard for process-wide network initialization
// (WSAStartup on Windows, SIGPIPE suppression on POSIX).
class NetworkContext {
public:
    NetworkContext();
    ~NetworkContext();

    NetworkContext(const NetworkContext&) = delete;
    NetworkContext& operator=(const NetworkContext&) = delete;
};

enum class IoStatus {
    Ok,          // Some bytes were transferred.
    WouldBlock,  // The operation would block; retry when the socket is ready.
    Closed,      // The peer performed an orderly shutdown.
    Error        // A fatal socket error occurred.
};

struct IoResult {
    IoStatus status = IoStatus::Error;
    std::size_t bytes = 0;
};

// Input/output descriptor for poll().
struct PollItem {
    SocketHandle socket = kInvalidSocket;
    bool wantRead = false;
    bool wantWrite = false;

    bool readable = false;
    bool writable = false;
    bool failed = false;
};

// Creates a listening TCP socket bound to host:port. Throws std::runtime_error on failure.
SocketHandle createListener(const std::string& host, std::uint16_t port, int backlog = 128);

// Accepts a pending connection. Returns kInvalidSocket if nothing is pending.
SocketHandle acceptClient(SocketHandle listener, std::string* peerAddress = nullptr);

// Opens a TCP connection. In non-blocking mode `inProgress` is set when the
// handshake has started but not yet completed. Returns kInvalidSocket on failure.
SocketHandle connectTo(const std::string& host, std::uint16_t port, bool nonBlocking, bool& inProgress);

bool setNonBlocking(SocketHandle socket);
void setNoDelay(SocketHandle socket);

// Returns the pending SO_ERROR value of a socket (0 when healthy).
int pendingError(SocketHandle socket);

IoResult receive(SocketHandle socket, char* data, std::size_t size);
IoResult transmit(SocketHandle socket, const char* data, std::size_t size);

void shutdownWrite(SocketHandle socket);
void closeSocket(SocketHandle socket);

// Waits for readiness events. Returns the number of ready sockets, 0 on timeout, -1 on error.
int poll(std::vector<PollItem>& items, int timeoutMs);

std::uint16_t localPort(SocketHandle socket);

int lastError();
std::string describeError(int code);

}  // namespace tlb::net
