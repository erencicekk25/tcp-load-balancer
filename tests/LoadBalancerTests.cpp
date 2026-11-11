// Tests for the Round-Robin scheduler and the load balancer event loop.
// The end-to-end cases run real TCP backends on ephemeral loopback ports.

#include "LoadBalancer.hpp"
#include "SocketUtils.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;
using tlb::net::SocketHandle;

int g_failures = 0;
int g_checks = 0;

#define CHECK(condition)                                                                   \
    do {                                                                                   \
        ++g_checks;                                                                        \
        if (!(condition)) {                                                                \
            ++g_failures;                                                                  \
            std::cerr << "  FAILED " << __FILE__ << ":" << __LINE__ << ": " #condition "\n"; \
        }                                                                                  \
    } while (false)

#define CHECK_EQ(actual, expected)                                                    \
    do {                                                                              \
        ++g_checks;                                                                   \
        const auto& actualValue = (actual);                                           \
        const auto& expectedValue = (expected);                                       \
        if (!(actualValue == expectedValue)) {                                        \
            ++g_failures;                                                             \
            std::cerr << "  FAILED " << __FILE__ << ":" << __LINE__ << ": " #actual   \
                      << " == " #expected "\n    actual:   " << actualValue           \
                      << "\n    expected: " << expectedValue << "\n";                 \
        }                                                                             \
    } while (false)

struct TestCase {
    const char* name;
    void (*body)();
};

std::vector<TestCase>& registry() {
    static std::vector<TestCase> tests;
    return tests;
}

struct Registrar {
    Registrar(const char* name, void (*body)()) { registry().push_back({name, body}); }
};

#define TEST(name)                                  \
    void name();                                    \
    const Registrar registrar_##name(#name, &name); \
    void name()

// ---------------------------------------------------------------------------
// Blocking socket helpers
// ---------------------------------------------------------------------------

bool sendAll(SocketHandle socket, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const auto result = tlb::net::transmit(socket, data.data() + sent, data.size() - sent);
        if (result.status == tlb::net::IoStatus::Ok) {
            sent += result.bytes;
        } else if (result.status == tlb::net::IoStatus::WouldBlock) {
            std::vector<tlb::net::PollItem> items{{socket, false, true}};
            tlb::net::poll(items, 50);
        } else {
            return false;
        }
    }
    return true;
}

// Reads until the peer closes the connection or the timeout expires.
std::string readUntilClosed(SocketHandle socket, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::string data;
    char buffer[16 * 1024];
    while (std::chrono::steady_clock::now() < deadline) {
        std::vector<tlb::net::PollItem> items{{socket, true, false}};
        if (tlb::net::poll(items, 50) <= 0) {
            continue;
        }
        const auto result = tlb::net::receive(socket, buffer, sizeof(buffer));
        if (result.status == tlb::net::IoStatus::Ok) {
            data.append(buffer, result.bytes);
        } else if (result.status != tlb::net::IoStatus::WouldBlock) {
            break;
        }
    }
    return data;
}

// Connects to the balancer, sends a payload, half-closes and returns the reply.
std::string request(std::uint16_t port, const std::string& payload) {
    bool inProgress = false;
    const SocketHandle socket = tlb::net::connectTo("127.0.0.1", port, false, inProgress);
    if (socket == tlb::net::kInvalidSocket) {
        return {};
    }
    std::string reply;
    if (sendAll(socket, payload)) {
        tlb::net::shutdownWrite(socket);
        reply = readUntilClosed(socket, 10s);
    }
    tlb::net::closeSocket(socket);
    return reply;
}

// Backend that reads the whole request (until half-close), then answers with
// "<name>:<request>" and closes. Serves connections one at a time.
class EchoBackend {
public:
    explicit EchoBackend(std::string name) : name_(std::move(name)) {
        listener_ = tlb::net::createListener("127.0.0.1", 0);
        port_ = tlb::net::localPort(listener_);
        thread_ = std::thread(&EchoBackend::serve, this);
    }

    ~EchoBackend() {
        running_ = false;
        thread_.join();
        tlb::net::closeSocket(listener_);
    }

    EchoBackend(const EchoBackend&) = delete;
    EchoBackend& operator=(const EchoBackend&) = delete;

    std::uint16_t port() const { return port_; }
    int served() const { return served_.load(); }

private:
    void serve() {
        while (running_) {
            std::vector<tlb::net::PollItem> items{{listener_, true, false}};
            if (tlb::net::poll(items, 50) <= 0 || !items[0].readable) {
                continue;
            }
            const SocketHandle client = tlb::net::acceptClient(listener_);
            if (client == tlb::net::kInvalidSocket) {
                continue;
            }
            const std::string body = readUntilClosed(client, 10s);
            sendAll(client, name_ + ":" + body);
            tlb::net::closeSocket(client);
            ++served_;
        }
    }

    std::string name_;
    SocketHandle listener_ = tlb::net::kInvalidSocket;
    std::uint16_t port_ = 0;
    std::atomic<bool> running_{true};
    std::atomic<int> served_{0};
    std::thread thread_;
};

// Runs a LoadBalancer on a background thread for the lifetime of the object.
class RunningBalancer {
public:
    explicit RunningBalancer(std::vector<std::uint16_t> backendPorts) {
        tlb::LoadBalancerConfig config;
        config.listenHost = "127.0.0.1";
        config.listenPort = 0;
        config.connectTimeout = 1000ms;
        config.backendCooldown = 60s;
        config.pollIntervalMs = 20;
        for (const auto port : backendPorts) {
            config.backends.push_back({"127.0.0.1", port});
        }
        balancer_ = std::make_unique<tlb::LoadBalancer>(std::move(config));
        balancer_->start();
        thread_ = std::thread([this] { balancer_->run(); });
    }

    ~RunningBalancer() {
        balancer_->stop();
        thread_.join();
    }

    RunningBalancer(const RunningBalancer&) = delete;
    RunningBalancer& operator=(const RunningBalancer&) = delete;

    tlb::LoadBalancer& operator*() { return *balancer_; }
    tlb::LoadBalancer* operator->() { return balancer_.get(); }

private:
    std::unique_ptr<tlb::LoadBalancer> balancer_;
    std::thread thread_;
};

// Returns a loopback port with nothing listening on it.
std::uint16_t closedPort() {
    const SocketHandle listener = tlb::net::createListener("127.0.0.1", 0);
    const std::uint16_t port = tlb::net::localPort(listener);
    tlb::net::closeSocket(listener);
    return port;
}

std::string prefixOf(const std::string& reply) {
    return reply.substr(0, reply.find(':'));
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

TEST(schedulerCyclesThroughAllBackends) {
    tlb::RoundRobinScheduler scheduler(3);
    const auto any = [](std::size_t) { return true; };
    std::vector<std::size_t> picks;
    for (int i = 0; i < 7; ++i) {
        picks.push_back(scheduler.next(any).value_or(99));
    }
    const std::vector<std::size_t> expected{0, 1, 2, 0, 1, 2, 0};
    CHECK(picks == expected);
}

TEST(schedulerSkipsIneligibleBackends) {
    tlb::RoundRobinScheduler scheduler(4);
    const auto notOne = [](std::size_t index) { return index != 1; };
    std::vector<std::size_t> picks;
    for (int i = 0; i < 6; ++i) {
        picks.push_back(scheduler.next(notOne).value_or(99));
    }
    const std::vector<std::size_t> expected{0, 2, 3, 0, 2, 3};
    CHECK(picks == expected);
}

TEST(schedulerReturnsNothingWhenAllVetoed) {
    tlb::RoundRobinScheduler scheduler(3);
    CHECK(!scheduler.next([](std::size_t) { return false; }).has_value());

    tlb::RoundRobinScheduler empty(0);
    CHECK(!empty.next([](std::size_t) { return true; }).has_value());
}

// ---------------------------------------------------------------------------
// End-to-end
// ---------------------------------------------------------------------------

TEST(distributesConnectionsInRoundRobinOrder) {
    EchoBackend alpha("alpha");
    EchoBackend beta("beta");
    EchoBackend gamma("gamma");
    RunningBalancer balancer({alpha.port(), beta.port(), gamma.port()});

    std::vector<std::string> order;
    for (int i = 0; i < 6; ++i) {
        const std::string reply = request(balancer->listenPort(), "ping" + std::to_string(i));
        CHECK_EQ(reply.substr(reply.find(':') + 1), "ping" + std::to_string(i));
        order.push_back(prefixOf(reply));
    }
    const std::vector<std::string> expected{"alpha", "beta", "gamma", "alpha", "beta", "gamma"};
    CHECK(order == expected);
    CHECK_EQ(alpha.served(), 2);
    CHECK_EQ(beta.served(), 2);
    CHECK_EQ(gamma.served(), 2);

    const auto stats = balancer->snapshot();
    CHECK_EQ(stats.size(), std::size_t(3));
    for (const auto& backend : stats) {
        CHECK(backend.healthy);
        CHECK_EQ(backend.totalConnections, std::uint64_t(2));
        CHECK_EQ(backend.failedConnections, std::uint64_t(0));
    }
}

TEST(failsOverWhenBackendIsDown) {
    const std::uint16_t deadPort = closedPort();
    EchoBackend live("live");
    RunningBalancer balancer({deadPort, live.port()});

    for (int i = 0; i < 4; ++i) {
        const std::string reply = request(balancer->listenPort(), "hello");
        CHECK_EQ(reply, std::string("live:hello"));
    }
    CHECK_EQ(live.served(), 4);

    const auto stats = balancer->snapshot();
    CHECK_EQ(stats.size(), std::size_t(2));
    CHECK(!stats[0].healthy);
    CHECK(stats[0].failedConnections >= 1);
    CHECK(stats[1].healthy);
    CHECK_EQ(stats[1].totalConnections, std::uint64_t(4));
}

TEST(relaysLargePayloadsIntact) {
    EchoBackend backend("big");
    RunningBalancer balancer({backend.port()});

    std::string payload(1024 * 1024, '\0');
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<char>('a' + (i * 7) % 26);
    }
    const std::string reply = request(balancer->listenPort(), payload);
    CHECK_EQ(reply.size(), payload.size() + 4);
    CHECK(reply == "big:" + payload);

    const auto stats = balancer->snapshot();
    CHECK_EQ(stats[0].bytesToBackend, std::uint64_t(payload.size()));
    CHECK_EQ(stats[0].bytesToClient, std::uint64_t(payload.size() + 4));
}

TEST(stopsPromptly) {
    EchoBackend backend("x");
    tlb::LoadBalancerConfig config;
    config.listenHost = "127.0.0.1";
    config.listenPort = 0;
    config.backends.push_back({"127.0.0.1", backend.port()});
    tlb::LoadBalancer balancer(std::move(config));
    balancer.start();
    CHECK(balancer.isRunning());
    CHECK(balancer.listenPort() != 0);

    std::thread loop([&balancer] { balancer.run(); });
    const auto started = std::chrono::steady_clock::now();
    balancer.stop();
    loop.join();
    CHECK(std::chrono::steady_clock::now() - started < 2s);
    CHECK(!balancer.isRunning());
}

}  // namespace

int main() {
    tlb::net::NetworkContext network;
    for (const auto& test : registry()) {
        const int failuresBefore = g_failures;
        test.body();
        std::cout << (g_failures == failuresBefore ? "[ PASS ] " : "[ FAIL ] ") << test.name << '\n';
    }
    std::cout << '\n'
              << registry().size() << " tests, " << g_checks << " checks, " << g_failures
              << " failure(s)\n";
    return g_failures == 0 ? 0 : 1;
}
