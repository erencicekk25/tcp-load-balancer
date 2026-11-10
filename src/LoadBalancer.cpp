#include "LoadBalancer.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <utility>

namespace tlb {

namespace {

// Upper bound of connections accepted per loop iteration, so that a burst of
// new clients cannot starve sessions that already have data in flight.
constexpr int kAcceptBatchLimit = 64;

}  // namespace

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

// One direction of a proxied connection: bytes read from `source` wait here
// until they can be written to `sink`.
struct LoadBalancer::Pipe {
    explicit Pipe(std::size_t capacity) : buffer(capacity) {}

    std::vector<char> buffer;
    std::size_t head = 0;
    std::size_t tail = 0;
    bool sourceClosed = false;  // EOF received from the reading side.
    bool sinkShutdown = false;  // FIN already forwarded to the writing side.

    std::size_t pending() const { return tail - head; }
    std::size_t freeSpace() const { return buffer.size() - tail; }
    bool wantsRead() const { return !sourceClosed && freeSpace() > 0; }
    bool wantsWrite() const { return pending() > 0; }

    void consume(std::size_t count) {
        head += count;
        if (head == tail) {
            head = tail = 0;
        } else if (head > buffer.size() / 2) {
            std::memmove(buffer.data(), buffer.data() + head, pending());
            tail -= head;
            head = 0;
        }
    }
};

struct LoadBalancer::Session {
    enum class State { Connecting, Streaming };

    Session(std::uint64_t sessionId, net::SocketHandle clientSocket, std::string peerAddress,
            std::size_t backendCount, std::size_t bufferSize)
        : id(sessionId),
          client(clientSocket),
          peer(std::move(peerAddress)),
          attempted(backendCount, false),
          upstream(bufferSize),
          downstream(bufferSize) {}

    std::uint64_t id;
    net::SocketHandle client;
    net::SocketHandle backend = net::kInvalidSocket;
    std::string peer;

    State state = State::Connecting;
    std::optional<std::size_t> backendIndex;
    std::vector<bool> attempted;
    Clock::time_point connectDeadline{};
    bool established = false;
    bool finished = false;

    Pipe upstream;    // client  -> backend
    Pipe downstream;  // backend -> client
};

struct LoadBalancer::PollTarget {
    enum class Role { Listener, Client, Backend };

    Session* session = nullptr;
    Role role = Role::Listener;
};

// ---------------------------------------------------------------------------
// BackendEndpoint / RoundRobinScheduler
// ---------------------------------------------------------------------------

std::string BackendEndpoint::toString() const {
    return host + ":" + std::to_string(port);
}

RoundRobinScheduler::RoundRobinScheduler(std::size_t backendCount) : backendCount_(backendCount) {}

std::optional<std::size_t> RoundRobinScheduler::next(const std::function<bool(std::size_t)>& isEligible) {
    for (std::size_t step = 0; step < backendCount_; ++step) {
        const std::size_t candidate = cursor_;
        cursor_ = (cursor_ + 1) % backendCount_;
        if (isEligible(candidate)) {
            return candidate;
        }
    }
    return std::nullopt;
}

// ---------------------------------------------------------------------------
// LoadBalancer: lifecycle
// ---------------------------------------------------------------------------

LoadBalancer::LoadBalancer(LoadBalancerConfig config)
    : config_(std::move(config)), scheduler_(config_.backends.size()) {
    if (config_.backends.empty()) {
        throw std::invalid_argument("LoadBalancer requires at least one backend");
    }
    if (config_.bufferSize == 0) {
        throw std::invalid_argument("bufferSize must be greater than zero");
    }

    backends_.reserve(config_.backends.size());
    for (const auto& endpoint : config_.backends) {
        Backend backend;
        backend.endpoint = endpoint;
        backend.stats.endpoint = endpoint.toString();
        backends_.push_back(std::move(backend));
    }
}

LoadBalancer::~LoadBalancer() {
    stop();
    closeAllSessions();
    net::closeSocket(listener_);
}

void LoadBalancer::start() {
    if (listener_ != net::kInvalidSocket) {
        throw std::logic_error("LoadBalancer::start() called twice");
    }

    listener_ = net::createListener(config_.listenHost, config_.listenPort);
    if (!net::setNonBlocking(listener_)) {
        net::closeSocket(listener_);
        listener_ = net::kInvalidSocket;
        throw std::runtime_error("unable to switch the listener to non-blocking mode");
    }

    boundPort_ = net::localPort(listener_);
    running_ = true;

    log("listening on " + config_.listenHost + ":" + std::to_string(boundPort_) + " with " +
        std::to_string(backends_.size()) + " backend(s), strategy: round-robin");
}

void LoadBalancer::stop() noexcept {
    running_ = false;
}

bool LoadBalancer::isRunning() const noexcept {
    return running_;
}

std::uint16_t LoadBalancer::listenPort() const noexcept {
    return boundPort_;
}

std::vector<BackendStats> LoadBalancer::snapshot() const {
    std::lock_guard<std::mutex> lock(backendsMutex_);
    const auto now = Clock::now();

    std::vector<BackendStats> result;
    result.reserve(backends_.size());
    for (const auto& backend : backends_) {
        BackendStats stats = backend.stats;
        stats.healthy = backend.unavailableUntil <= now;
        result.push_back(std::move(stats));
    }
    return result;
}

// ---------------------------------------------------------------------------
// LoadBalancer: event loop
// ---------------------------------------------------------------------------

void LoadBalancer::run() {
    if (listener_ == net::kInvalidSocket) {
        throw std::logic_error("LoadBalancer::start() must be called before run()");
    }

    std::vector<net::PollItem> items;
    std::vector<PollTarget> targets;

    while (running_) {
        items.clear();
        targets.clear();

        items.push_back({listener_, true, false});
        targets.push_back({nullptr, PollTarget::Role::Listener});

        for (const auto& owned : sessions_) {
            Session& session = *owned;

            if (session.state == Session::State::Connecting) {
                // Writability signals completion (or failure) of a non-blocking connect.
                items.push_back({session.backend, false, true});
                targets.push_back({&session, PollTarget::Role::Backend});
                continue;
            }

            const bool clientRead = session.upstream.wantsRead();
            const bool clientWrite = session.downstream.wantsWrite();
            if (clientRead || clientWrite) {
                items.push_back({session.client, clientRead, clientWrite});
                targets.push_back({&session, PollTarget::Role::Client});
            }

            const bool backendRead = session.downstream.wantsRead();
            const bool backendWrite = session.upstream.wantsWrite();
            if (backendRead || backendWrite) {
                items.push_back({session.backend, backendRead, backendWrite});
                targets.push_back({&session, PollTarget::Role::Backend});
            }
        }

        const int ready = net::poll(items, config_.pollIntervalMs);
        if (ready < 0) {
            log("poll() failed: " + net::describeError(net::lastError()));
            continue;
        }

        for (std::size_t i = 0; ready > 0 && i < items.size(); ++i) {
            const net::PollItem& item = items[i];
            if (!item.readable && !item.writable && !item.failed) {
                continue;
            }

            const PollTarget& target = targets[i];
            switch (target.role) {
                case PollTarget::Role::Listener:
                    acceptPendingClients();
                    break;
                case PollTarget::Role::Client:
                    if (!target.session->finished) {
                        handleClientEvent(*target.session, item);
                    }
                    break;
                case PollTarget::Role::Backend:
                    if (!target.session->finished) {
                        handleBackendEvent(*target.session, item);
                    }
                    break;
            }
        }

        expireConnectAttempts();

        sessions_.erase(std::remove_if(sessions_.begin(), sessions_.end(),
                                       [](const std::unique_ptr<Session>& session) { return session->finished; }),
                        sessions_.end());
    }

    closeAllSessions();
    log("event loop stopped");
}

void LoadBalancer::acceptPendingClients() {
    for (int accepted = 0; accepted < kAcceptBatchLimit; ++accepted) {
        std::string peer;
        const net::SocketHandle client = net::acceptClient(listener_, &peer);
        if (client == net::kInvalidSocket) {
            return;  // Nothing left in the accept queue.
        }

        if (!net::setNonBlocking(client)) {
            log("dropping client " + peer + ": cannot enable non-blocking mode");
            net::closeSocket(client);
            continue;
        }
        net::setNoDelay(client);

        auto session = std::make_unique<Session>(nextSessionId_++, client, std::move(peer), backends_.size(),
                                                 config_.bufferSize);

        if (connectToNextBackend(*session)) {
            sessions_.push_back(std::move(session));
        } else {
            finishSession(*session, "no backend available");
        }
    }
}

// ---------------------------------------------------------------------------
// LoadBalancer: backend selection & connection management
// ---------------------------------------------------------------------------

std::optional<std::size_t> LoadBalancer::pickBackend(const Session& session) {
    std::lock_guard<std::mutex> lock(backendsMutex_);
    const auto now = Clock::now();

    // First pass: healthy backends this session has not tried yet.
    auto choice = scheduler_.next([&](std::size_t index) {
        return !session.attempted[index] && backends_[index].unavailableUntil <= now;
    });

    // Second pass: every backend is cooling down, so optimistically try one
    // anyway. It may have recovered before its cooldown expired.
    if (!choice) {
        choice = scheduler_.next([&](std::size_t index) { return !session.attempted[index]; });
    }
    return choice;
}

bool LoadBalancer::connectToNextBackend(Session& session) {
    while (const auto index = pickBackend(session)) {
        session.attempted[*index] = true;
        const BackendEndpoint& endpoint = backends_[*index].endpoint;

        bool inProgress = false;
        const net::SocketHandle socket = net::connectTo(endpoint.host, endpoint.port, true, inProgress);
        if (socket == net::kInvalidSocket) {
            log("session #" + std::to_string(session.id) + ": immediate connect failure to " + endpoint.toString());
            markBackendDown(*index);
            continue;
        }

        session.backend = socket;
        session.backendIndex = index;
        session.state = Session::State::Connecting;
        session.connectDeadline = Clock::now() + config_.connectTimeout;

        if (!inProgress) {
            // Loopback connects can complete synchronously.
            net::PollItem synthetic{socket, false, true};
            synthetic.writable = true;
            handleConnectProgress(session, synthetic);
        }
        return true;
    }
    return false;
}

void LoadBalancer::handleConnectProgress(Session& session, const net::PollItem& item) {
    const int error = net::pendingError(session.backend);
    if (error != 0 || item.failed || !item.writable) {
        handleConnectFailure(session, error != 0 ? net::describeError(error) : "connection refused");
        return;
    }

    session.state = Session::State::Streaming;
    session.established = true;
    net::setNoDelay(session.backend);

    {
        std::lock_guard<std::mutex> lock(backendsMutex_);
        Backend& backend = backends_[*session.backendIndex];
        backend.unavailableUntil = Clock::time_point{};
        ++backend.stats.totalConnections;
        ++backend.stats.activeConnections;
    }

    log("session #" + std::to_string(session.id) + ": " + session.peer + " -> " +
        backends_[*session.backendIndex].endpoint.toString());
}

void LoadBalancer::handleConnectFailure(Session& session, const std::string& reason) {
    const std::size_t index = *session.backendIndex;
    log("session #" + std::to_string(session.id) + ": backend " + backends_[index].endpoint.toString() +
        " unreachable (" + reason + "), failing over");

    net::closeSocket(session.backend);
    session.backend = net::kInvalidSocket;
    session.backendIndex.reset();
    markBackendDown(index);

    if (!connectToNextBackend(session)) {
        finishSession(session, "all backends are unreachable");
    }
}

void LoadBalancer::markBackendDown(std::size_t index) {
    std::lock_guard<std::mutex> lock(backendsMutex_);
    Backend& backend = backends_[index];
    backend.unavailableUntil = Clock::now() + config_.backendCooldown;
    ++backend.stats.failedConnections;
}

void LoadBalancer::expireConnectAttempts() {
    const auto now = Clock::now();
    for (const auto& owned : sessions_) {
        Session& session = *owned;
        if (!session.finished && session.state == Session::State::Connecting && now >= session.connectDeadline) {
            handleConnectFailure(session, "connect timeout");
        }
    }
}

// ---------------------------------------------------------------------------
// LoadBalancer: data relay
// ---------------------------------------------------------------------------

void LoadBalancer::handleClientEvent(Session& session, const net::PollItem& item) {
    if (item.readable && !readInto(session, session.client, session.upstream, true)) {
        return;
    }
    if (item.writable && !flushFrom(session, session.downstream, session.client)) {
        return;
    }
    propagateHalfClose(session);
}

void LoadBalancer::handleBackendEvent(Session& session, const net::PollItem& item) {
    if (session.state == Session::State::Connecting) {
        handleConnectProgress(session, item);
        return;
    }

    if (item.readable && !readInto(session, session.backend, session.downstream, false)) {
        return;
    }
    if (item.writable && !flushFrom(session, session.upstream, session.backend)) {
        return;
    }
    propagateHalfClose(session);
}

bool LoadBalancer::readInto(Session& session, net::SocketHandle source, Pipe& pipe, bool towardsBackend) {
    if (!pipe.wantsRead()) {
        return true;
    }

    const net::IoResult result = net::receive(source, pipe.buffer.data() + pipe.tail, pipe.freeSpace());
    switch (result.status) {
        case net::IoStatus::Ok: {
            pipe.tail += result.bytes;
            {
                std::lock_guard<std::mutex> lock(backendsMutex_);
                BackendStats& stats = backends_[*session.backendIndex].stats;
                (towardsBackend ? stats.bytesToBackend : stats.bytesToClient) += result.bytes;
            }
            // Opportunistic write: forward immediately instead of waiting for the next poll round.
            const net::SocketHandle sink = towardsBackend ? session.backend : session.client;
            return flushFrom(session, pipe, sink);
        }
        case net::IoStatus::Closed:
            pipe.sourceClosed = true;
            return true;
        case net::IoStatus::WouldBlock:
            return true;
        case net::IoStatus::Error:
            break;
    }

    finishSession(session, std::string(towardsBackend ? "client" : "backend") + " read error");
    return false;
}

bool LoadBalancer::flushFrom(Session& session, Pipe& pipe, net::SocketHandle sink) {
    if (!pipe.wantsWrite()) {
        return true;
    }

    const net::IoResult result = net::transmit(sink, pipe.buffer.data() + pipe.head, pipe.pending());
    if (result.status == net::IoStatus::Ok) {
        pipe.consume(result.bytes);
        return true;
    }
    if (result.status == net::IoStatus::WouldBlock) {
        return true;
    }

    finishSession(session, "write error");
    return false;
}

void LoadBalancer::propagateHalfClose(Session& session) {
    // Forward a FIN only after every buffered byte has been delivered, so that
    // TCP half-close semantics are preserved end-to-end.
    if (session.upstream.sourceClosed && !session.upstream.wantsWrite() && !session.upstream.sinkShutdown) {
        net::shutdownWrite(session.backend);
        session.upstream.sinkShutdown = true;
    }
    if (session.downstream.sourceClosed && !session.downstream.wantsWrite() && !session.downstream.sinkShutdown) {
        net::shutdownWrite(session.client);
        session.downstream.sinkShutdown = true;
    }
    if (session.upstream.sinkShutdown && session.downstream.sinkShutdown) {
        finishSession(session, "completed");
    }
}

void LoadBalancer::finishSession(Session& session, const std::string& reason) {
    if (session.finished) {
        return;
    }
    session.finished = true;

    net::closeSocket(session.client);
    net::closeSocket(session.backend);
    session.client = net::kInvalidSocket;
    session.backend = net::kInvalidSocket;

    if (session.established && session.backendIndex) {
        std::lock_guard<std::mutex> lock(backendsMutex_);
        --backends_[*session.backendIndex].stats.activeConnections;
    }

    if (reason != "completed") {
        log("session #" + std::to_string(session.id) + " closed: " + reason);
    }
}

void LoadBalancer::closeAllSessions() {
    for (const auto& session : sessions_) {
        finishSession(*session, "shutting down");
    }
    sessions_.clear();
}

void LoadBalancer::log(const std::string& message) const {
    if (config_.logger) {
        config_.logger(message);
    }
}

}  // namespace tlb
