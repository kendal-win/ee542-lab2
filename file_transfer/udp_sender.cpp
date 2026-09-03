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

// Max payload we'll ever allocate - large enough for 9001 MTU
#define MAX_CHUNK_SIZE 9000

int main(int argc, char *argv[])
{
    if (argc != 5) {
        fprintf(stderr, "usage: %s hostname port file_to_send chunk_size\n", argv[0]);
        exit(1);
    }

    const char *hostname = argv[1];
    const char *port = argv[2];
    const char *filepath = argv[3];
    int chunk_size = atoi(argv[4]);

    if (chunk_size <= 0 || chunk_size > MAX_CHUNK_SIZE) {
        fprintf(stderr, "chunk_size must be between 1 and %d\n", MAX_CHUNK_SIZE);
        exit(1);
    }

    FILE *fp = fopen(filepath, "rb");
    if (!fp) {
        perror("fopen");
        exit(1);
    }

    struct addrinfo hints, *servinfo, *p;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    int rv = getaddrinfo(hostname, port, &hints, &servinfo);
    if (rv != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return 1;
    }

    int sockfd = -1;
    for (p = servinfo; p != NULL; p = p->ai_next) {
        sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (sockfd == -1) {
            perror("sender: socket");
            continue;
        }
        break;
    }

    if (p == NULL) {
        fprintf(stderr, "sender: failed to create socket\n");
        return 2;
    }

    // create a larger buffer to send data
    int sndbuf = 4 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    char buf[MAX_CHUNK_SIZE];
    size_t bytes_read;
    long total_bytes_sent = 0;
    long total_packets = 0;

    printf("sender: starting transfer of %s to %s:%s (chunk size %d)\n",
           filepath, hostname, port, chunk_size);

    auto start_time = std::chrono::steady_clock::now(); //start timing the transmission rate

    while ((bytes_read = fread(buf, 1, chunk_size, fp)) > 0) {
        ssize_t sent = sendto(sockfd, buf, bytes_read, 0,
                               p->ai_addr, p->ai_addrlen);
        if (sent == -1) {
            perror("sender: sendto");
            fclose(fp);
            close(sockfd);
            exit(1);
        }
        total_bytes_sent += sent;
        total_packets++;
    }

    if (ferror(fp)) {
        perror("sender: fread");
    }

    //use an empty datagram, with length 0 to signal the end of the file
    sendto(sockfd, NULL, 0, 0, p->ai_addr, p->ai_addrlen);

    auto end_time = std::chrono::steady_clock::now(); //stop timing the transmissionr ate
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    double mbps = (total_bytes_sent * 8.0 / 1000000.0) / elapsed_sec;

    printf("sender: done. sent %ld bytes in %ld packets\n", total_bytes_sent, total_packets);
    printf("sender: elapsed time (first byte sent -> EOF marker sent): %.4f sec\n", elapsed_sec);
    printf("sender: throughput: %.2f Mbps\n", mbps);

    freeaddrinfo(servinfo);
    fclose(fp);
    close(sockfd);

    return 0;
}