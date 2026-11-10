#pragma once

#include "SocketUtils.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace tlb {

struct BackendEndpoint {
    std::string host;
    std::uint16_t port = 0;

    std::string toString() const;
};

struct LoadBalancerConfig {
    std::string listenHost = "127.0.0.1";
    std::uint16_t listenPort = 8080;
    std::vector<BackendEndpoint> backends;

    // Maximum time a non-blocking connect() may take before the backend is considered down.
    std::chrono::milliseconds connectTimeout{2000};

    // How long a failed backend is skipped by the scheduler before it is tried again.
    std::chrono::milliseconds backendCooldown{5000};

    // Per-direction buffer size of every proxied session.
    std::size_t bufferSize = 64 * 1024;

    // Upper bound for a single poll() wait; controls how quickly stop() is honoured.
    int pollIntervalMs = 100;

    // Optional sink for diagnostic messages. Called from the event-loop thread.
    std::function<void(const std::string&)> logger;
};

struct BackendStats {
    std::string endpoint;
    bool healthy = true;
    std::uint64_t totalConnections = 0;
    std::uint64_t activeConnections = 0;
    std::uint64_t failedConnections = 0;
    std::uint64_t bytesToBackend = 0;
    std::uint64_t bytesToClient = 0;
};

// Classic Round-Robin: hands out indices 0, 1, ..., n-1, 0, 1, ... while
// letting the caller veto candidates (e.g. unhealthy or already-tried backends).
class RoundRobinScheduler {
public:
    explicit RoundRobinScheduler(std::size_t backendCount);

    std::optional<std::size_t> next(const std::function<bool(std::size_t)>& isEligible);

private:
    std::size_t backendCount_;
    std::size_t cursor_ = 0;
};

// Single-threaded, event-driven TCP proxy. Every client connection is paired
// with one backend connection chosen by the Round-Robin scheduler, and bytes
// are relayed in both directions through non-blocking sockets multiplexed with poll().
class LoadBalancer {
public:
    explicit LoadBalancer(LoadBalancerConfig config);
    ~LoadBalancer();

    LoadBalancer(const LoadBalancer&) = delete;
    LoadBalancer& operator=(const LoadBalancer&) = delete;

    // Binds the listening socket. Throws std::runtime_error on failure.
    void start();

    // Runs the event loop on the calling thread until stop() is invoked.
    void run();

    // Thread-safe; makes run() return within roughly one poll interval.
    void stop() noexcept;

    bool isRunning() const noexcept;
    std::uint16_t listenPort() const noexcept;

    // Thread-safe snapshot of per-backend counters.
    std::vector<BackendStats> snapshot() const;

private:
    using Clock = std::chrono::steady_clock;

    struct Backend {
        BackendEndpoint endpoint;
        Clock::time_point unavailableUntil{};
        BackendStats stats;
    };

    struct Pipe;
    struct Session;
    struct PollTarget;

    void acceptPendingClients();
    bool connectToNextBackend(Session& session);
    void handleConnectProgress(Session& session, const net::PollItem& item);
    void handleConnectFailure(Session& session, const std::string& reason);
    void handleClientEvent(Session& session, const net::PollItem& item);
    void handleBackendEvent(Session& session, const net::PollItem& item);
    void expireConnectAttempts();
    void finishSession(Session& session, const std::string& reason);
    void closeAllSessions();

    bool readInto(Session& session, net::SocketHandle source, Pipe& pipe, bool towardsBackend);
    bool flushFrom(Session& session, Pipe& pipe, net::SocketHandle sink);
    void propagateHalfClose(Session& session);

    std::optional<std::size_t> pickBackend(const Session& session);
    void markBackendDown(std::size_t index);
    void log(const std::string& message) const;

    LoadBalancerConfig config_;
    RoundRobinScheduler scheduler_;
    std::vector<Backend> backends_;
    mutable std::mutex backendsMutex_;

    std::vector<std::unique_ptr<Session>> sessions_;
    net::SocketHandle listener_ = net::kInvalidSocket;
    std::uint16_t boundPort_ = 0;
    std::uint64_t nextSessionId_ = 1;
    std::atomic<bool> running_{false};
};

}  // namespace tlb
