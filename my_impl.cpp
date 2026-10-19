#include <iostream>
#include <atomic>
#include <chrono>


namespace Config{
    constexpr int PORT = 8080;
    constexpr int BACKLOG = 32768;
    constexpr int NUM_WORKERS = 5;
    constexpr int MAX_EVENTS = 2048;
    constexpr int READ_BUFFER_SIZE = 4086;
}

static const char HTTP_RESPONSE[] =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 13\r\n"
    "Connection: keep-alive\r\n"
    "\r\n"
    "Hello, World!";
static const size_t HTTP_RESPONSE_LEN = sizeof(HTTP_RESPONSE) - 1;

struct ServerMetrics{
    std::atomic<int64_t> active_connections{0};
    std::atomic<int64_t> total_requests{0};
    std::chrono::steady_clock::time_point start_time;
};

static ServerMetrics metrics;
static std::atomic<bool> running {true};