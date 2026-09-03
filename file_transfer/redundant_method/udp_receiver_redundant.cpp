//redundant version where each chunk is processed for multiple recv
//added a seq number
//added a command line argument for time out [sec]
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <vector>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <chrono>

// Max payload we'll ever allocate - large enough for 9001 MTU
#define MAX_CHUNK_SIZE 9000
#define HEADER_SIZE (sizeof(uint32_t) * 2)
 
int main(int argc, char *argv[])
{
    if (argc != 5) {
        fprintf(stderr, "usage: %s port output_file chunk_size timeout_sec\n", argv[0]);
        exit(1);
    }
 
    const char *port = argv[1];
    const char *outpath = argv[2];
    int chunk_size = atoi(argv[3]);
    int timeout_sec = atoi(argv[4]);
 
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
    hints.ai_family = AF_INET;
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
 
    int rcvbuf = 4 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
 
    //Timeout so don't hang forever if all copies of a chunk got dropped
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
 
    char packet[HEADER_SIZE + MAX_CHUNK_SIZE];
    struct sockaddr_storage their_addr;
    socklen_t addr_len;
 
    uint32_t total_chunks = 0;
    std::vector<bool> received; //resized after calculating total_chunks
    long unique_chunks_received = 0;
    long total_packets_received = 0;
    long duplicate_packets = 0;
 
    bool started = false;
    std::chrono::steady_clock::time_point start_time, end_time;
 
    printf("receiver: waiting for data on port %s (timeout %ds after last packet)...\n",
           port, timeout_sec);
 
    while (true) {
        addr_len = sizeof their_addr;
        ssize_t numbytes = recvfrom(sockfd, packet, sizeof(packet), 0, (struct sockaddr *)&their_addr, &addr_len);
        if (numbytes == -1) {
            // Timed out waiting for more data and assume sender is done (or remaining chunks are permanently lost).
            printf("receiver: timed out waiting for more packets, stopping.\n");
            break;
        }
        if ((size_t)numbytes < HEADER_SIZE) {
            continue; //too of a short packet, ignore
        }
 
        if (!started) {
            start_time = std::chrono::steady_clock::now();
            started = true;
        }
        end_time = std::chrono::steady_clock::now();
        total_packets_received++;
 
        uint32_t seq, tchunks;
        memcpy(&seq, packet, sizeof(seq));
        memcpy(&tchunks, packet + sizeof(seq), sizeof(tchunks));
 
        if (total_chunks == 0) {
            total_chunks = tchunks;
            received.assign(total_chunks, false);
        }
 
        if (seq >= total_chunks) {
            continue; // bogus sequence number, ignore
        }
 
        size_t payload_len = numbytes - HEADER_SIZE;
 
        if (received[seq]) {
            duplicate_packets++;
            continue; //already have this chunk, skip writing it again
        }
 
        fseek(out, (long)seq * chunk_size, SEEK_SET);
        fwrite(packet + HEADER_SIZE, 1, payload_len, out);
        received[seq] = true;
        unique_chunks_received++;
 
        if (unique_chunks_received == total_chunks) {
            printf("receiver: all %u chunks received.\n", total_chunks);
            break;
        }
    }
 
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    double mbps = elapsed_sec > 0 ? ((unique_chunks_received * (double)chunk_size * 8.0) / 1000000.0) / elapsed_sec : 0;
 
    printf("receiver: unique chunks received: %ld / %u\n", unique_chunks_received, total_chunks);
    printf("receiver: total packets received: %ld (duplicates: %ld)\n", total_packets_received, duplicate_packets);
    printf("receiver: elapsed time (first byte -> last unique byte): %.4f sec\n", elapsed_sec);
    printf("receiver: throughput (unique payload): %.2f Mbps\n", mbps);
 
    if (unique_chunks_received < total_chunks) {
        fprintf(stderr, "receiver: WARNING -- %ld chunk(s) missing, file is incomplete.\n", (long)total_chunks - unique_chunks_received);
    }
 
    fclose(out);
    close(sockfd);
    return 0;
}