#include <iostream>
#include <atomic>
#include <chrono>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <cstdio>

namespace Config{
    constexpr int PORT = 8080;
    constexpr int BACKLOG = 32768; // max length of kernel queue for TCP conn waiting to be accepted.
    constexpr int NUM_WORKERS = 5;
    constexpr int MAX_EVENTS = 2048;
    constexpr int READ_BUFFER_SIZE = 4086; // in bytes
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
    int flags = fcntl(fd, F_GETFL, 0); // F_GETFL means 'get the file status flag'. fcntl returns a bitmask upon success, or -1 
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK); // setting the bitmask flag to enable non blocking, and returns 0 upon success;
}

// this helper is to be called on every newly accepted client socket
static void configure_client_socket(int fd){
    set_nonblocking(fd);
    int flag = 1; // enable flag
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)); // Need to disable Nagle's algorithm which buffers outgoing packets, creating artificial delays for small HTTP payloads 
}

/* note: Need to remember 3 important epoll functions:
1. epoll_create1() - creating the epoll instance
2. epoll_wait() - block IO until events are ready
3. epoll_ctl() - adds, updates, or deletes sockets
*/
class WorkerThread{
    int epoll_fd{-1};
    std::thread thread_handle;

    void init(){
        epoll_fd = epoll_create1(EPOLL_CLOEXEC);
        if(epoll_fd < 0){
            perror("epoll_create1 failed");
            exit(EXIT_FAILURE);
        }
    }

    void start(){
        thread_handle = std::thread(&WorkerThread::run, this);
    }

    void add_client(int client_fd){
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLONESHOT ; // edge triggered, notify when client TCP half-closes read, prevent race condition with EPOLLONESHOT
        ev.data.fd = client_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) == -1) perror("epoll_ctl error");
    }

private:
    void run(){
        std::vector<epoll_event> events(Config::MAX_EVENTS); // think.. a reusable scratchpad for temporarily storing a client's metadata
        char buffer[Config::READ_BUFFER_SIZE];

        while(running.load(std::memory_order_relaxed)){
            int nfds = epoll_wait(epoll_fd, events.data(), Config::MAX_EVENTS, 100);
            if (nfds <= 0) continue;
            // if epoll_wait returns value > 0 (say x), it means only x events have been populated by the kernel.
            for(int i = 0; i<nfds; i++){
                int client_fd = events[i].data.fs;
                uint32_t ev_mask = events[i].events;

                if (ev_mask & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)){
                    close_connection(client_fd);
                    continue;
                }

                if(ev_mask & EPOLLIN){
                    bool close_req = false;

                    while(true){
                        ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer));
                        if (bytes_read > 0){
                        metrics.total_requests.fetch_add(1, std::memory_order_relaxed);

                        //sending the fixed response here
                        ssize_t bytes_written = write(client_fd, HTTP_RESPONSE, HTTP_RESPONSE_LEN);
                        // check if write() failed cuz of actual error and not cuz of full buffer (EAGAIN/EWOULDBLOCK - same thing) 
                        if (bytes_written < 0 && (errno != EAGAIN && errno != EWOULDBLOCK)){
                            close_req = true;
                            break;
                        }
                    }
                    else if (bytes_read == 0){
                        close_req = true;
                        break;
                    }
                    else{
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        close_req = true;
                        break;
                    }
                    }

                    if(close_req){
                        close_connection(client_fd);
                    }
                    else{
                        // re-arm the edge triggered descriptor cuz we used EPOLLONESHOT, which disarms a descriptor as soon as it delivers an event.
                        // Do NOT EPOLL_CTL_ADD. The FD already exists in the interest list of the kernel (just disarmed).
                        epoll_event ev{};
                        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLONESHOT ;
                        ev.data.fd = client_fd;
                        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client_fd, &ev);
                    }
                }
            }
        }
    }

    void close_connection(int fd){
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
        metrics.active_connections.fetch_sub(1, std::memory_order_relaxed);
    }
};

class ListenerThread{}; // connection accepting thread
int main(){return 0;}