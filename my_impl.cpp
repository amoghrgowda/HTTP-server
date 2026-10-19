#include <iostream>
#include <atomic>
#include <chrono>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

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

//helper func for turning sockets into non blocking for epoll usage
static int set_nonblocking(int fd){
    int flags = fcntl(fd, F_GETFL, 0); // F_GETFL means 'get the file status flag'
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK); // setting the bitmask flag to enable non blocking, and returns 0 upon success;
}

// this helper is to be called on every newly accepted client socket
static void configure_client_socket(int fd){
    set_nonblocking(fd);
    int flag = 1; // enable flag
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)); // Need to disable Nagle's algorithm which buffers outgoing packets, creating artificial delays for small HTTP payloads 
}

class WorkerThread{}; //  A worker that shall own epoll instance and an OS thread.
class ListenerThread{}; // connection accepting thread
int main(){return 0;}