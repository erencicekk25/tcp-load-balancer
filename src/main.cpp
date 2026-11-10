// CLI demo for the TCP load balancer.
//
// Spins up a handful of simulated backend servers, puts the Round-Robin load
// balancer in front of them and fires test traffic through it, printing which
// backend served each request. Can optionally kill a backend mid-run to show
// automatic failover, or stay alive for manual testing with netcat/telnet.

#include "LoadBalancer.hpp"
#include "SocketUtils.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;
using tlb::net::SocketHandle;

// ---------------------------------------------------------------------------
// Console output
// ---------------------------------------------------------------------------

std::mutex outputMutex;

void logLine(const std::string& source, const std::string& message) {
    const auto now = std::chrono::system_clock::now();
    const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000;
    const std::time_t seconds = std::chrono::system_clock::to_time_t(now);

    std::lock_guard<std::mutex> lock(outputMutex);
    const std::tm local = *std::localtime(&seconds);
    std::cout << '[' << std::put_time(&local, "%H:%M:%S") << '.' << std::setw(3) << std::setfill('0') << millis
              << std::setfill(' ') << "] [" << std::left << std::setw(9) << source << std::right << "] " << message
              << '\n';
    std::cout.flush();
}

// ---------------------------------------------------------------------------
// Blocking socket helpers used by the simulated backends and the test client
// ---------------------------------------------------------------------------

bool sendAll(SocketHandle socket, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const auto result = tlb::net::transmit(socket, data.data() + sent, data.size() - sent);
        if (result.status == tlb::net::IoStatus::Ok) {
            sent += result.bytes;
        } else if (result.status == tlb::net::IoStatus::WouldBlock) {
            std::vector<tlb::net::PollItem> items{{socket, false, true}};
            tlb::net::poll(items, 100);
        } else {
            return false;
        }
    }
    return true;
}

std::optional<std::string> readLine(SocketHandle socket, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::string line;
    char ch = 0;

    while (std::chrono::steady_clock::now() < deadline) {
        std::vector<tlb::net::PollItem> items{{socket, true, false}};
        if (tlb::net::poll(items, 50) <= 0 || !items[0].readable) {
            continue;
        }

        const auto result = tlb::net::receive(socket, &ch, 1);
        if (result.status == tlb::net::IoStatus::Ok) {
            if (ch == '\n') {
                return line;
            }
            if (ch != '\r') {
                line.push_back(ch);
            }
        } else if (result.status != tlb::net::IoStatus::WouldBlock) {
            break;
        }
    }
    return line.empty() ? std::nullopt : std::optional<std::string>(line);
}

// ---------------------------------------------------------------------------
// Simulated backend: a tiny line-based TCP server
// ---------------------------------------------------------------------------

class SimulatedBackend {
public:
    SimulatedBackend(std::string name, std::uint16_t port) : name_(std::move(name)), port_(port) {}

    ~SimulatedBackend() { stop(); }

    SimulatedBackend(const SimulatedBackend&) = delete;
    SimulatedBackend& operator=(const SimulatedBackend&) = delete;

    void start() {
        listener_ = tlb::net::createListener("127.0.0.1", port_);
        running_ = true;
        acceptThread_ = std::thread(&SimulatedBackend::acceptLoop, this);
        logLine(name_, "online at 127.0.0.1:" + std::to_string(port_));
    }

    void stop() {
        if (!running_.exchange(false)) {
            return;
        }
        if (acceptThread_.joinable()) {
            acceptThread_.join();
        }
        tlb::net::closeSocket(listener_);
        listener_ = tlb::net::kInvalidSocket;

        std::vector<std::thread> workers;
        {
            std::lock_guard<std::mutex> lock(workersMutex_);
            workers.swap(workers_);
        }
        for (auto& worker : workers) {
            worker.join();
        }
        logLine(name_, "offline");
    }

    const std::string& name() const { return name_; }
    std::uint16_t port() const { return port_; }
    std::uint64_t servedRequests() const { return served_; }

private:
    void acceptLoop() {
        while (running_) {
            std::vector<tlb::net::PollItem> items{{listener_, true, false}};
            if (tlb::net::poll(items, 100) <= 0 || !items[0].readable) {
                continue;
            }

            std::string peer;
            const SocketHandle client = tlb::net::acceptClient(listener_, &peer);
            if (client == tlb::net::kInvalidSocket) {
                continue;
            }

            std::lock_guard<std::mutex> lock(workersMutex_);
            workers_.emplace_back(&SimulatedBackend::serveClient, this, client);
        }
    }

    void serveClient(SocketHandle client) {
        std::string pending;
        char buffer[4096];

        while (running_) {
            std::vector<tlb::net::PollItem> items{{client, true, false}};
            const int ready = tlb::net::poll(items, 100);
            if (ready < 0) {
                break;
            }
            if (ready == 0 || !items[0].readable) {
                continue;
            }

            const auto result = tlb::net::receive(client, buffer, sizeof(buffer));
            if (result.status == tlb::net::IoStatus::WouldBlock) {
                continue;
            }
            if (result.status != tlb::net::IoStatus::Ok) {
                break;  // Peer closed the connection or an error occurred.
            }

            pending.append(buffer, result.bytes);
            std::size_t newline = 0;
            while ((newline = pending.find('\n')) != std::string::npos) {
                std::string request = pending.substr(0, newline);
                pending.erase(0, newline + 1);
                if (!request.empty() && request.back() == '\r') {
                    request.pop_back();
                }

                ++served_;
                const std::string reply = name_ + " (port " + std::to_string(port_) + ") handled: " + request + "\n";
                if (!sendAll(client, reply)) {
                    tlb::net::closeSocket(client);
                    return;
                }
            }
        }
        tlb::net::closeSocket(client);
    }

    std::string name_;
    std::uint16_t port_;
    SocketHandle listener_ = tlb::net::kInvalidSocket;
    std::atomic<bool> running_{false};
    std::atomic<std::uint64_t> served_{0};
    std::thread acceptThread_;
    std::vector<std::thread> workers_;
    std::mutex workersMutex_;
};

// ---------------------------------------------------------------------------
// Test client
// ---------------------------------------------------------------------------

std::optional<std::string> sendRequest(std::uint16_t port, const std::string& payload) {
    bool inProgress = false;
    const SocketHandle socket = tlb::net::connectTo("127.0.0.1", port, false, inProgress);
    if (socket == tlb::net::kInvalidSocket) {
        return std::nullopt;
    }

    std::optional<std::string> response;
    if (sendAll(socket, payload + "\n")) {
        response = readLine(socket, 3000ms);
    }
    tlb::net::closeSocket(socket);
    return response;
}

void runTrafficRound(std::uint16_t balancerPort, int firstRequest, int count, std::map<std::string, int>& hits) {
    for (int i = firstRequest; i < firstRequest + count; ++i) {
        const std::string payload = "request #" + std::to_string(i);
        const auto response = sendRequest(balancerPort, payload);

        if (!response) {
            logLine("client", payload + " -> FAILED (no response)");
            ++hits["<failed>"];
            continue;
        }

        logLine("client", payload + " -> " + *response);
        ++hits[response->substr(0, response->find(' '))];
        std::this_thread::sleep_for(50ms);
    }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

void printSummary(const std::vector<tlb::BackendStats>& stats, const std::map<std::string, int>& hits) {
    std::lock_guard<std::mutex> lock(outputMutex);

    std::cout << "\n=== Backend statistics (reported by the load balancer) ===\n";
    std::cout << std::left << std::setw(18) << "Endpoint" << std::setw(10) << "Status" << std::right << std::setw(8)
              << "Total" << std::setw(8) << "Active" << std::setw(8) << "Failed" << std::setw(12) << "Bytes in"
              << std::setw(12) << "Bytes out" << '\n';
    std::cout << std::string(76, '-') << '\n';

    for (const auto& entry : stats) {
        std::cout << std::left << std::setw(18) << entry.endpoint << std::setw(10) << (entry.healthy ? "UP" : "DOWN")
                  << std::right << std::setw(8) << entry.totalConnections << std::setw(8) << entry.activeConnections
                  << std::setw(8) << entry.failedConnections << std::setw(12) << entry.bytesToBackend << std::setw(12)
                  << entry.bytesToClient << '\n';
    }

    std::cout << "\n=== Request distribution (observed by the client) ===\n";
    int total = 0;
    for (const auto& [name, count] : hits) {
        total += count;
    }
    for (const auto& [name, count] : hits) {
        const int barLength = total > 0 ? (count * 40) / total : 0;
        std::cout << std::left << std::setw(12) << name << std::right << std::setw(4) << count << "  "
                  << std::string(static_cast<std::size_t>(barLength), '#') << '\n';
    }
    std::cout << std::endl;
}

// ---------------------------------------------------------------------------
// Command line
// ---------------------------------------------------------------------------

struct CliOptions {
    std::uint16_t listenPort = 8080;
    std::uint16_t backendBasePort = 9001;
    int backendCount = 3;
    int requestCount = 9;
    bool failoverDemo = false;
    bool interactive = false;
};

void printUsage(const char* program) {
    std::cout << "Usage: " << program << " [options]\n\n"
              << "Options:\n"
              << "  --port <n>           Load balancer listen port            (default: 8080)\n"
              << "  --backend-port <n>   First port of the simulated backends (default: 9001)\n"
              << "  --backends <n>       Number of simulated backends, 1-16   (default: 3)\n"
              << "  --requests <n>       Requests sent per traffic round      (default: 9)\n"
              << "  --failover           Stop the last backend and send a second round\n"
              << "  --interactive        Keep running for manual tests (e.g. `nc 127.0.0.1 8080`)\n"
              << "  --help               Show this message\n";
}

std::optional<CliOptions> parseArguments(int argc, char** argv) {
    CliOptions options;

    auto readNumber = [&](int& index, long min, long max) -> std::optional<long> {
        if (index + 1 >= argc) {
            std::cerr << "Missing value for " << argv[index] << '\n';
            return std::nullopt;
        }
        try {
            const long value = std::stol(argv[++index]);
            if (value < min || value > max) {
                throw std::out_of_range("range");
            }
            return value;
        } catch (const std::exception&) {
            std::cerr << "Invalid value for " << argv[index - 1] << ": expected " << min << "-" << max << '\n';
            return std::nullopt;
        }
    };

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        std::optional<long> value;

        if (arg == "--port") {
            if (!(value = readNumber(i, 1, 65535))) return std::nullopt;
            options.listenPort = static_cast<std::uint16_t>(*value);
        } else if (arg == "--backend-port") {
            if (!(value = readNumber(i, 1, 65535 - 16))) return std::nullopt;
            options.backendBasePort = static_cast<std::uint16_t>(*value);
        } else if (arg == "--backends") {
            if (!(value = readNumber(i, 1, 16))) return std::nullopt;
            options.backendCount = static_cast<int>(*value);
        } else if (arg == "--requests") {
            if (!(value = readNumber(i, 1, 10000))) return std::nullopt;
            options.requestCount = static_cast<int>(*value);
        } else if (arg == "--failover") {
            options.failoverDemo = true;
        } else if (arg == "--interactive") {
            options.interactive = true;
        } else if (arg == "--help" || arg == "-h") {
            printUsage(argv[0]);
            std::exit(0);
        } else {
            std::cerr << "Unknown option: " << arg << "\n\n";
            printUsage(argv[0]);
            return std::nullopt;
        }
    }
    return options;
}

}  // namespace

int main(int argc, char** argv) {
    const auto options = parseArguments(argc, argv);
    if (!options) {
        return 1;
    }

    try {
        tlb::net::NetworkContext network;

        // 1) Bring up the simulated backend fleet.
        std::vector<std::unique_ptr<SimulatedBackend>> backends;
        tlb::LoadBalancerConfig config;
        config.listenHost = "127.0.0.1";
        config.listenPort = options->listenPort;
        config.connectTimeout = 500ms;  // Fail over quickly in the demo.
        config.logger = [](const std::string& message) { logLine("balancer", message); };

        for (int i = 0; i < options->backendCount; ++i) {
            const auto port = static_cast<std::uint16_t>(options->backendBasePort + i);
            auto backend = std::make_unique<SimulatedBackend>("backend-" + std::to_string(i + 1), port);
            backend->start();
            config.backends.push_back({"127.0.0.1", port});
            backends.push_back(std::move(backend));
        }

        // 2) Start the load balancer on its own event-loop thread.
        tlb::LoadBalancer balancer(config);
        balancer.start();

        std::thread eventLoop([&balancer] {
            try {
                balancer.run();
            } catch (const std::exception& error) {
                logLine("balancer", std::string("fatal: ") + error.what());
            }
        });

        // 3) Generate traffic or wait for manual tests.
        std::map<std::string, int> hits;

        if (options->interactive) {
            logLine("main", "interactive mode - try: nc 127.0.0.1 " + std::to_string(balancer.listenPort()));
            logLine("main", "press ENTER to shut down");
            std::string ignored;
            std::getline(std::cin, ignored);
        } else {
            logLine("main", "round 1: sending " + std::to_string(options->requestCount) + " requests");
            runTrafficRound(balancer.listenPort(), 1, options->requestCount, hits);

            if (options->failoverDemo && backends.size() > 1) {
                logLine("main", "simulating an outage: stopping " + backends.back()->name());
                backends.back()->stop();

                logLine("main", "round 2: sending " + std::to_string(options->requestCount) + " requests");
                runTrafficRound(balancer.listenPort(), options->requestCount + 1, options->requestCount, hits);
            }
        }

        // Give in-flight sessions a moment to close so the counters settle.
        std::this_thread::sleep_for(200ms);
        const auto stats = balancer.snapshot();

        balancer.stop();
        eventLoop.join();
        for (auto& backend : backends) {
            backend->stop();
        }

        if (!options->interactive) {
            printSummary(stats, hits);
        }
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
