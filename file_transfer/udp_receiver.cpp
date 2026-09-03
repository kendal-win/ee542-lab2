#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <chrono>

#define MAX_CHUNK_SIZE 9000

int main(int argc, char *argv[])
{
    if (argc != 4) {
        fprintf(stderr, "usage: %s port output_file chunk_size\n", argv[0]);
        exit(1);
    }

    const char *port = argv[1];
    const char *outpath = argv[2];
    int chunk_size = atoi(argv[3]);

    if (chunk_size <= 0 || chunk_size > MAX_CHUNK_SIZE) {
        fprintf(stderr, "chunk_size must be between 1 and %d\n", MAX_CHUNK_SIZE);
        exit(1);
    }

    FILE *out = fopen(outpath, "wb");
    if (!out) {
        perror("fopen");
        exit(1);
    }

    struct addrinfo hints, *servinfo, *p;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET; //ipv4
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags = AI_PASSIVE;

    int rv = getaddrinfo(NULL, port, &hints, &servinfo);
    if (rv != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return 1;
    }

    int sockfd = -1;
    for (p = servinfo; p != NULL; p = p->ai_next) {
        sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (sockfd == -1) {
            perror("listener: socket");
            continue;
        }
        if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            perror("listener: bind");
            continue;
        }
        break;
    }

    if (p == NULL) {
        fprintf(stderr, "listener: failed to bind socket\n");
        return 2;
    }
    freeaddrinfo(servinfo);

    // create a larger buffer to receive data
    int rcvbuf = 4 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    char buf[MAX_CHUNK_SIZE];
    struct sockaddr_storage their_addr;
    socklen_t addr_len;
    long total_bytes = 0;
    long total_packets = 0;
    bool started = false;

    std::chrono::steady_clock::time_point start_time, end_time;

    printf("receiver: waiting for data on port %s...\n", port);

    while (true) {
        addr_len = sizeof their_addr;
        ssize_t numbytes = recvfrom(sockfd, buf, chunk_size, 0,
                                     (struct sockaddr *)&their_addr, &addr_len);
        if (numbytes == -1) {
            perror("recvfrom");
            break;
        }

        if (numbytes == 0) {
            // EOF marker.
            end_time = std::chrono::steady_clock::now();
            break;
        }

        if (!started) {
            start_time = std::chrono::steady_clock::now();
            started = true;
        }

        fwrite(buf, 1, numbytes, out);
        total_bytes += numbytes;
        total_packets++;
        end_time = std::chrono::steady_clock::now(); // updated each packet, final value = last data packet
    }

    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    double mbps = elapsed_sec > 0 ? (total_bytes * 8.0 / 1000000.0) / elapsed_sec : 0;

    printf("receiver: done. received %ld bytes in %ld packets, wrote to %s\n",
           total_bytes, total_packets, outpath);
    printf("receiver: elapsed time (first byte recv -> last byte recv): %.4f sec\n", elapsed_sec);
    printf("receiver: throughput: %.2f Mbps\n", mbps);

    fclose(out);
    close(sockfd);

    return 0;
}