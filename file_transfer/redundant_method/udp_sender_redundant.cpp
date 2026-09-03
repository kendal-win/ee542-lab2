//redundant version where each chunk is sent extra times incase 1 or more are lost
//add a seq number each packet
//add an extra command line parameter for number of resends
//usage: ./udp_sender_redundant <hostname> <port> <file> <chunk_size> <redundancy>
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
#define HEADER_SIZE (sizeof(uint32_t) * 2)

int main(int argc, char *argv[])
{
    if (argc != 6) {
        fprintf(stderr, "usage: %s hostname port file chunk_size redundancy\n", argv[0]);
        exit(1);
    }

    const char *hostname = argv[1];
    const char *port = argv[2];
    const char *filepath = argv[3];
    int chunk_size = atoi(argv[4]);
    int redundancy = atoi(argv[5]);

    if (chunk_size <= 0 || chunk_size > MAX_CHUNK_SIZE) {
        fprintf(stderr, "chunk_size must be between 1 and %d\n", MAX_CHUNK_SIZE);
        exit(1);
    }
    if (redundancy <= 0) {
        fprintf(stderr, "redundancy must be >= 1\n");
        exit(1);
    }

    FILE *fp = fopen(filepath, "rb");
    if (!fp) {
        perror("fopen");
        exit(1);
    }

    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
 
    uint32_t total_chunks = (uint32_t)((file_size + chunk_size - 1) / chunk_size);

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

    
    char packet[HEADER_SIZE + MAX_CHUNK_SIZE];
    char *payload = packet + HEADER_SIZE;
 
    long total_bytes_sent = 0;
    long total_packets_sent = 0;

    printf("sender: file_size=%ld bytes, chunk_size=%d, total_chunks=%u, redundancy=%d\n", file_size, chunk_size, total_chunks, redundancy);
 
    auto start_time = std::chrono::steady_clock::now(); //start timing the transmission rate

    //loop through the number of chunks we're sending
    for (int pass = 0; pass < redundancy; pass++) {
        fseek(fp, 0, SEEK_SET);
 
        for (uint32_t seq = 0; seq < total_chunks; seq++) {
            size_t bytes_read = fread(payload, 1, chunk_size, fp);
            if (bytes_read == 0) break; // shouldn't happen given total_chunks calc
 
            uint32_t seq_net = seq;
            uint32_t total_net = total_chunks;
            memcpy(packet, &seq_net, sizeof(seq_net));
            memcpy(packet + sizeof(seq_net), &total_net, sizeof(total_net));
 
            ssize_t sent = sendto(sockfd, packet, HEADER_SIZE + bytes_read, 0, p->ai_addr, p->ai_addrlen);
            if (sent == -1) {
                perror("sender: sendto");
                continue;
            }
            total_packets_sent++;
 
            if (pass == 0) {
                total_bytes_sent += bytes_read; // count unique payload bytes once
            }
        }
    }
 
    auto end_time = std::chrono::steady_clock::now(); //end timing
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    double mbps = (total_bytes_sent * 8.0 / 1000000.0) / elapsed_sec;
 
    printf("sender: done. %u unique chunks, %ld total packets sent (incl. redundancy)\n", total_chunks, total_packets_sent);
    printf("sender: elapsed time: %.4f sec\n", elapsed_sec);
    printf("sender: effective throughput (unique payload bytes): %.2f Mbps\n", mbps);
 
    freeaddrinfo(servinfo);
    fclose(fp);
    close(sockfd);
    return 0;
}
 