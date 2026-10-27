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
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)); // Need to disable Nagle's algorithm which buffers outgoing packets, creating artificial delays for small HTTP payloads.
    // Read MAN pages to know the function signature (IMPORTANT!!).
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

class ListenerThread{
public:
    int listen_fd{-1};
    std::thread thread_handle;
    std::vector<WorkerThread>* workers{nullptr};

    void init(int port, std::vector<WorkerThread>* worker_pool){
        workers = worker_pool;
        listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if(listen_fd<0){
            perror("socket creation failed");
            exit(EXIT_FAILURE);
        }

        int opt = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)); // allow the server to reuse the port, immediately after restart (if any).
        // Typically, The kernel waits 1-2 minutes before it frees a port (address) for use in the TIME_WAIT state to catch lingering packets.
        // We have disabled this behaviour above.

        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = 0; // all interfaces (0.0.0.0).
        address.sin_port = htons(port);

        if(bind(listen_fd, (struct sockaddr*)&address, sizeof(address)) < 0){
            perror("bind failed");
            exit(EXIT_FAILURE);
        }

        if(listen(listen_fd, Config::BACKLOG) < 0){
            perror("Listen failed");
            exit(EXIT_FAILURE);
        }
        
        void start(){
            thread_handle = std::thread(&ListenerThread::run, this);
        }
    }

private:
    void run(){
        int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = listen_fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev);

        std::vector<epoll_event> events(64);
        size_t next_worker = 0;

        while(running.load(std::memory_order_relaxed)){
            int nfds = epoll_wait(epoll_fd, events.data(), event.size(), 100);
            for(int i = 0; i < nfds; i++){
                if(events[i].data.fd == listen_fd){
                    while(true){
                        sockaddr_in client_addr{};
                        socklen_t client_len = sizeof(client_addr);
                        int client_fd = accept4(listen_fd, (struct sockaddr*)&client_addr, &client_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
                        if (client_fd < 0){
                            break;
                        }

                        configure_client_socket(client_fd);
                        metrics.active_connections.fetch_add(1, std::memory_order_relaxed);

                        (*workers)[next_worker].add_client(client_fd);
                        next_worker = (next_worker + 1) % workers->size();
                    }
                }
            }
        }
        close(epoll_fd);
    }
}; // connection accepting thread
int main(){return 0;}