// Usage: ./nack_sender <hostname> <port> <file> <chunk_size> <group_size> <redundancy>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <vector>
#include <algorithm>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <chrono>

#define MAX_CHUNK_SIZE 9000
#define TYPE_DATA 0
#define TYPE_PARITY 1
#define TYPE_NACK 2
#define TYPE_DONE 3
#define TYPE_RECOVERY_DONE 4
#define HEADER_SIZE (1 + 4 + 4 + 8)
#define NACK_HEADER_SIZE (4 + 4)

struct NackHeader {
    uint32_t nack_id;
    uint32_t missing_count;
};

static bool parse_nack(
    const char *packet,
    size_t packet_len,
    uint32_t &nack_id,
    std::vector<uint32_t> &missing)
{
    if(packet_len < NACK_HEADER_SIZE) {
        return false;
    }

    NackHeader header;
    memcpy(&header, packet, NACK_HEADER_SIZE);

    size_t expected_size = 
        NACK_HEADER_SIZE +
        (size_t)header.missing_count * sizeof(uint32_t);

    if (packet_len < expected_size) {
        return false;
    }

    nack_id = header.nack_id;

    missing.resize(header.missing_count);

    memcpy(
        missing.data(),
        packet + NACK_HEADER_SIZE,
        header.missing_count * sizeof(uint32_t)
    );

    return true;

}

static int sockfd;
static struct addrinfo *dest;
static struct sockaddr_storage receiver_addr;
static socklen_t receiver_addr_len;
static uint32_t last_nack_id = UINT32_MAX;
static char *packet;
static char *payload;
static uint32_t total_chunks;
static uint64_t file_size;
static int chunk_size;
static long total_packets_sent = 0;

static void fill_header(uint8_t type, uint32_t seq_or_gid)
{
    packet[0] = (char)type;
    memcpy(packet + 1, &seq_or_gid, sizeof(seq_or_gid));
    memcpy(packet + 1 + 4, &total_chunks, sizeof(total_chunks));
    memcpy(packet + 1 + 4 + 4, &file_size, sizeof(file_size));
}

static void retransmit_chunks(
    FILE *fp,
    const std::vector<uint32_t> &missing,
    int chunk_size,
    uint32_t total_chunks,
    uint64_t file_size)
{
    std::vector<char> packet_buf(HEADER_SIZE + MAX_CHUNK_SIZE);
    char *packet = packet_buf.data();
    char *payload = packet + HEADER_SIZE;

    for (uint32_t seq : missing) {
        if(seq >= total_chunks) {
            continue;
        }
        
        memset(payload, 0, chunk_size);

        fseek(fp, (long)seq * chunk_size, SEEK_SET);
        size_t bytes_read = fread(payload, 1, chunk_size, fp);

        uint8_t type = TYPE_DATA;

        memcpy(packet, &type, sizeof(type));
        memcpy(packet + 1, &seq, sizeof(seq));
        memcpy(packet + 1 + 4, &total_chunks, sizeof(total_chunks));
        memcpy(packet + 1 + 4 + 4, &file_size, sizeof(file_size));

        sendto(
            sockfd,
            packet,
            HEADER_SIZE + bytes_read,
            0,
            dest->ai_addr,
            dest->ai_addrlen
        );

        //uint8_t recovery_done = TYPE_RECOVERY_DONE;

        //sendto(sockfd, &recovery_done, sizeof(recovery_done), 0, dest->ai_addr, dest->ai_addrlen);

        usleep(100);

    }
}

enum ControlMessage {
    CONTROL_NONE,
    CONTROL_NACK,
    CONTROL_DONE
};

static ControlMessage receive_control(uint32_t &nack_id, std::vector<uint32_t> &missing)
{
    char control_packet[65536];
    struct sockaddr_storage sender_addr;
    socklen_t sender_addr_len = sizeof(sender_addr);

    ssize_t numbytes = recvfrom(
        sockfd,
        control_packet,
        sizeof(control_packet),
        MSG_DONTWAIT,
        (struct sockaddr *)&sender_addr,
        &sender_addr_len
    );
    if(numbytes < 0) {
        return CONTROL_NONE;
    }
    
    //DONE is a single byte containing TYPE_DONE
    if(numbytes == 1 && (uint8_t)control_packet[0] == TYPE_DONE) {
        return CONTROL_DONE;
    }

    //Otherwise, try to parse it as a NACK
    if (parse_nack(control_packet, (size_t)numbytes, nack_id, missing)) {
        return CONTROL_NACK;
    }

    return CONTROL_NONE;
}

int main(int argc, char *argv[])
{
    if (argc != 7) {
        fprintf(stderr, "usage: %s hostname port file chunk_size group_size redundancy\n", argv[0]);
        exit(1);
    }

    const char *hostname = argv[1];
    const char *port = argv[2];
    const char *filepath = argv[3];
    chunk_size = atoi(argv[4]);
    uint32_t group_size = (uint32_t)atoi(argv[5]);
    int redundancy = atoi(argv[6]);

    if (chunk_size <= 0 || chunk_size > MAX_CHUNK_SIZE) {
        fprintf(stderr, "chunk_size must be between 1 and %d\n", MAX_CHUNK_SIZE);
        exit(1);
    }
    if (group_size == 0) {
        fprintf(stderr, "group_size must be >= 1\n");
        exit(1);
    }
    if (redundancy < 1) {
        fprintf(stderr, "redundancy must be >= 1\n");
        exit(1);
    }

    FILE *fp = fopen(filepath, "rb");
    if (!fp) {
        perror("fopen");
        exit(1);
    }

    // Scratch file to hold each group's parity so passes 2..R can
    // resend it without recomputing (auto-deleted when closed).
    FILE *parity_scratch = tmpfile();
    if (!parity_scratch) {
        perror("tmpfile");
        exit(1);
    }

    fseek(fp, 0, SEEK_END);
    file_size = (uint64_t)ftell(fp);
    fseek(fp, 0, SEEK_SET);

    total_chunks = (uint32_t)((file_size + chunk_size - 1) / chunk_size);
    uint32_t num_groups = (total_chunks + group_size - 1) / group_size;

    struct addrinfo hints, *servinfo, *p;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    int rv = getaddrinfo(hostname, port, &hints, &servinfo);
    if (rv != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return 1;
    }

    sockfd = -1;
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
    dest = p;

    int sndbuf = 4 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    std::vector<char> packet_buf(HEADER_SIZE + MAX_CHUNK_SIZE);
    packet = packet_buf.data();
    payload = packet + HEADER_SIZE;

    std::vector<char> parity_accum(chunk_size);

    long total_data_bytes_sent = 0;

    printf("sender: file_size=%lu, chunk_size=%d, total_chunks=%u, group_size=%u, num_groups=%u, redundancy=%d\n", (unsigned long)file_size, chunk_size, total_chunks, group_size, num_groups, redundancy);

    auto start_time = std::chrono::steady_clock::now();

    // Pass 1: send data , build + send + save parity of groups
    for (uint32_t g = 0; g < num_groups; g++) {
        uint32_t start = g * group_size;
        uint32_t end = start + group_size;
        if (end > total_chunks) end = total_chunks;

        std::fill(parity_accum.begin(), parity_accum.end(), 0);

        for (uint32_t seq = start; seq < end; seq++) {
            memset(payload, 0, chunk_size);
            fseek(fp, (long)seq * chunk_size, SEEK_SET);
            size_t bytes_read = fread(payload, 1, chunk_size, fp);

            fill_header(TYPE_DATA, seq);
            sendto(sockfd, packet, HEADER_SIZE + bytes_read, 0, dest->ai_addr, dest->ai_addrlen);
            total_packets_sent++;
            total_data_bytes_sent += bytes_read;
            usleep(120);

            for (int i = 0; i < chunk_size; i++) parity_accum[i] ^= payload[i];
        }

        fill_header(TYPE_PARITY, g);
        memcpy(payload, parity_accum.data(), chunk_size);
        sendto(sockfd, packet, HEADER_SIZE + chunk_size, 0, dest->ai_addr, dest->ai_addrlen);
        total_packets_sent++;

        fseek(parity_scratch, (long)g * chunk_size, SEEK_SET);
        fwrite(parity_accum.data(), 1, chunk_size, parity_scratch);
    }

    // Passes 2..R: resend everything, interleaved as full passes
    for (int pass = 2; pass <= redundancy; pass++) {
        for (uint32_t seq = 0; seq < total_chunks; seq++) {
            memset(payload, 0, chunk_size);
            fseek(fp, (long)seq * chunk_size, SEEK_SET);
            size_t bytes_read = fread(payload, 1, chunk_size, fp);

            fill_header(TYPE_DATA, seq);
            sendto(sockfd, packet, HEADER_SIZE + bytes_read, 0, dest->ai_addr, dest->ai_addrlen);
            total_packets_sent++;
        }

        for (uint32_t g = 0; g < num_groups; g++) {
            fseek(parity_scratch, (long)g * chunk_size, SEEK_SET);
            fread(payload, 1, chunk_size, parity_scratch);

            fill_header(TYPE_PARITY, g);
            sendto(sockfd, packet, HEADER_SIZE + chunk_size, 0, dest->ai_addr, dest->ai_addrlen);
            total_packets_sent++;
        }
    }

    // Wait for NACKs and retransmit requested chunks
    //Sender stays alive until the receiver sends DONE
    printf("sender: initial transmission complete. Waiting for NACKs or DONE...\n");

    while(true) {
        uint32_t nack_id;
        std::vector<uint32_t> missing;

        ControlMessage message = receive_control(nack_id, missing);

        if(message == CONTROL_DONE) {
            printf("sender: received DONE from receiver.\n");
            break;
        }
        if (message == CONTROL_NACK) {
            //Ignore duplicate copies of the same NACK.
            if (nack_id == last_nack_id) {
                printf("sender: ignoring duplicate NACK ID %u.\n", nack_id);
            } else {
                last_nack_id = nack_id;
                printf("sender:received NACK ID %u requesting %zu chunk(s).\n", nack_id, missing.size());
                retransmit_chunks(fp, missing, chunk_size, total_chunks, file_size);
                printf("sender: retransmitted %zu requested chunk(s).\n", missing.size());
            }
        }
        usleep(1000);
    }

    auto end_time = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    double mbps = (total_data_bytes_sent * 8.0 / 1000000.0) / elapsed_sec;
    long unique_packets = total_chunks + num_groups;
    double overhead_multiplier = (double)total_packets_sent / unique_packets;

    printf("sender: done. %u data + %u parity = %ld unique packets; %ld total sent (redundancy=%d)\n",
           total_chunks, num_groups, unique_packets, total_packets_sent, redundancy);
    printf("sender: total bandwidth overhead multiplier: %.2fx raw file size\n", overhead_multiplier);
    printf("sender: elapsed time: %.4f sec\n", elapsed_sec);
    printf("sender: effective throughput (unique data only): %.2f Mbps\n", mbps);

    freeaddrinfo(servinfo);
    fclose(fp);
    fclose(parity_scratch);
    close(sockfd);
    return 0;
}