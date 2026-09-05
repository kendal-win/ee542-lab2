//Usage: ./nack_receiver <port> <output_file> <chunk_size> <group_size> <timeout_sec>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <vector>
#include <unordered_map>
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

#define MAX_NACK_ENTRIES 360
#define NACK_RETRIES 1

static void send_nack(
    int sockfd,
    const struct sockaddr_storage &sender_addr,
    socklen_t sender_addr_len,
    uint32_t nack_id,
    const std::vector<uint32_t> &missing)
{
    NackHeader header;
    header.nack_id = nack_id;
    header.missing_count = (uint32_t)missing.size();

    std::vector<char> packet(
        NACK_HEADER_SIZE + missing.size() * sizeof(uint32_t)
    );

    memcpy(packet.data(), &header, NACK_HEADER_SIZE);

    memcpy(
        packet.data() + NACK_HEADER_SIZE,
        missing.data(),
        missing.size() * sizeof(uint32_t)
    );

    sendto(
        sockfd,
        packet.data(),
        packet.size(),
        0,
        (const struct sockaddr *)&sender_addr,
        sender_addr_len
    );
    usleep(120);
}

static std::vector<uint32_t> find_missing_chunks(
    const std::vector<bool> &received)
{
    std::vector<uint32_t> missing;

    for(uint32_t seq = 0; seq < received.size(); seq++) {
        if(!received[seq]) {
            missing.push_back(seq);
        }
    }
    return missing;
}

static void send_nacks(
    int sockfd,
    const struct sockaddr_storage &sender_addr,
    socklen_t sender_addr_len,
    const std::vector<uint32_t> &missing,
    uint32_t &next_nack_id)
{
    size_t offset = 0;

    while (offset < missing.size()) {
        size_t count = missing.size() - offset;

        if (count > MAX_NACK_ENTRIES) {
            count = MAX_NACK_ENTRIES;
        }

        std::vector<uint32_t> batch(
            missing.begin() + offset,
            missing.begin() + offset + count
        );

        for(int retry = 0; retry < NACK_RETRIES; retry++) {
            send_nack(
                sockfd,
                sender_addr,
                sender_addr_len,
                next_nack_id,
                batch
            );
        }

        next_nack_id++;
        offset += count;
    }
}

static void send_done(
    int sockfd,
    const struct sockaddr_storage &sender_addr,
    socklen_t sender_addr_len)
{
    uint8_t done = TYPE_DONE;
    for(int i = 0; i<5; i++) {
        sendto(sockfd, &done, sizeof(done), 0, (const struct sockaddr *)&sender_addr, sender_addr_len);
    }
    usleep(50000);
}

int main(int argc, char *argv[])
{
    if (argc != 6) {
        fprintf(stderr, "usage: %s port output_file chunk_size group_size timeout_sec\n", argv[0]);
        exit(1);
    }

    const char *port = argv[1];
    const char *outpath = argv[2];
    int chunk_size = atoi(argv[3]);
    uint32_t group_size = (uint32_t)atoi(argv[4]);
    int timeout_sec = atoi(argv[5]);
    int recovery_timeout_ms = 200;

    if (chunk_size <= 0 || chunk_size > MAX_CHUNK_SIZE) {
        fprintf(stderr, "chunk_size must be between 1 and %d\n", MAX_CHUNK_SIZE);
        exit(1);
    }
    if (group_size == 0) {
        fprintf(stderr, "group_size must be >= 1\n");
        exit(1);
    }

    // w+b so we can read back already-written chunks during recovery.
    FILE *out = fopen(outpath, "w+b");
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

    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    std::vector<char> packet(HEADER_SIZE + MAX_CHUNK_SIZE);
    struct sockaddr_storage their_addr;
    socklen_t addr_len;

    uint32_t total_chunks = 0;
    uint32_t num_groups = 0;
    uint64_t file_size = 0;
    std::vector<bool> received;
    std::unordered_map<uint32_t, std::vector<char>> parity_map;

    long unique_chunks_received = 0;
    long total_packets_received = 0;
    long data_packets = 0;
    long parity_packets = 0;
    int recovery_round = 0;
    uint32_t next_nack_id = 0;

    bool started = false;
    std::chrono::steady_clock::time_point start_time, end_time;

    printf("receiver: waiting for data on port %s (timeout %ds after last packet)...\n",
           port, timeout_sec);

    while (true) {
        addr_len = sizeof(their_addr);
        ssize_t numbytes = recvfrom(sockfd, packet.data(), packet.size(), 0,
                                     (struct sockaddr *)&their_addr, &addr_len);
        if (numbytes == -1) {
            printf("receiver: timed out waiting for more packets, moving to recovery pass.\n");
            break;
        }
        if ((size_t)numbytes < HEADER_SIZE) continue;

        if (!started) {
            start_time = std::chrono::steady_clock::now();
            started = true;
        }
        end_time = std::chrono::steady_clock::now();
        total_packets_received++;

        uint8_t type = packet[0];
        uint32_t seq_or_gid, tchunks;
        uint64_t fsize;
        memcpy(&seq_or_gid, packet.data() + 1, sizeof(seq_or_gid));
        memcpy(&tchunks, packet.data() + 1 + 4, sizeof(tchunks));
        memcpy(&fsize, packet.data() + 1 + 4 + 4, sizeof(fsize));

        if (total_chunks == 0) {
            total_chunks = tchunks;
            file_size = fsize;
            num_groups = (total_chunks + group_size - 1) / group_size;
            received.assign(total_chunks, false);
        }

        if (type == TYPE_DATA) {
            uint32_t seq = seq_or_gid;
            if (seq < total_chunks && !received[seq]) {
                size_t payload_len = numbytes - HEADER_SIZE;
                fseek(out, (long)seq * chunk_size, SEEK_SET);
                fwrite(packet.data() + HEADER_SIZE, 1, payload_len, out);
                received[seq] = true;
                unique_chunks_received++;
            }
            data_packets++;
        } else if (type == TYPE_PARITY) {
            uint32_t gid = seq_or_gid;
            if (gid < num_groups && parity_map.find(gid) == parity_map.end()) {
                parity_map[gid] = std::vector<char>(packet.data() + HEADER_SIZE,
                                                      packet.data() + HEADER_SIZE + chunk_size);
            }
            parity_packets++;
        }

        if (total_chunks > 0 && unique_chunks_received == (long)total_chunks) {
            printf("receiver: all %u chunks received directly (no recovery needed).\n", total_chunks);
            send_done(sockfd, their_addr, addr_len);
            printf("receiver: sent DONE to sender.\n");
            break;
        }
    }

    // Switch to a shorter timeout for retransmission/recovery.
    struct timeval recovery_tv;
    recovery_tv.tv_sec = recovery_timeout_ms / 1000;
    recovery_tv.tv_usec = (recovery_timeout_ms % 1000) * 1000;

    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &recovery_tv, sizeof(recovery_tv));

    //Recovery pass: repeatedly perform XOR recovery and request
    // retransmission of any chunks that are still missing.
    long recovered = 0;
    long unrecoverable = 0;

    while (total_chunks > 0 && unique_chunks_received < (long)total_chunks) {
        recovery_round++;
        auto round_start = std::chrono::steady_clock::now();

       // printf("\nreceiver: ===== recovery round %d =====\n", recovery_round);
        
        // -------------------------------------------------------------
        // XOR recovery pass
        // -------------------------------------------------------------
        for(uint32_t g = 0; g < num_groups; g++) {
            uint32_t start = g * group_size;
            uint32_t end = start + group_size;

            if (end > total_chunks) {
                end = total_chunks;
            }

            std::vector<uint32_t> group_missing;

            for (uint32_t seq = start; seq < end; seq++) {
                if (!received[seq]) {
                    group_missing.push_back(seq);
                }
            }

            if (group_missing.empty()) {
                continue;
            }

            // Our XOR parity can recover exactly one missing chunk.
            if (group_missing.size() == 1 && parity_map.find(g) != parity_map.end()) {
                uint32_t miss_seq = group_missing[0];
                std::vector<char> recon = parity_map[g];
                std::vector<char> buf(chunk_size, 0);
                for(uint32_t seq = start; seq < end; seq++) {
                    if(seq == miss_seq) {
                        continue;
                    }

                    size_t plen = (seq == total_chunks - 1) ? (size_t)(file_size - (uint64_t)seq * chunk_size) : (size_t)chunk_size;
                    std::fill(buf.begin(), buf.end(), 0);

                    fseek(out, (long)seq * chunk_size, SEEK_SET);

                    fread(buf.data(), 1, plen, out);

                    for (int i = 0; i < chunk_size; i++) {
                        recon[i] ^= buf[i];
                    }
                }

                size_t out_len = (miss_seq == total_chunks - 1) ? (size_t)(file_size - (uint64_t)miss_seq * chunk_size) : (size_t)chunk_size;
                
                fseek(out, (long)miss_seq * chunk_size, SEEK_SET);

                fwrite(recon.data(), 1, out_len, out);

                received[miss_seq] = true;
                unique_chunks_received++;
                recovered++;

               // printf("receiver: XOR recovered chunk %u (group %u).\n", miss_seq, g);
            }
        }

        // --------------------------------------------------------
        // Check whether the entire file is now complete.
        // --------------------------------------------------------
        if(unique_chunks_received == (long)total_chunks) {
            printf("receiver: all %u chunks received/recovered.\n", total_chunks);

            send_done(
                sockfd,
                their_addr,
                addr_len
            );

            printf("receiver: sent DONE to sender.\n");
            break;
        }

        // --------------------------------------------------------
        // Find chunks still missing after XOR recovery.
        // --------------------------------------------------------
        std::vector<uint32_t> missing = find_missing_chunks(received);
        printf("receiver: NACK requesting %zu chunks\n", missing.size());
        unrecoverable = (long)missing.size();
       // printf("receiver: %zu chunks still missing after XOR recovery.\n", missing.size());
        if (missing.empty()) {
            break;
        }

        // ------------------------------------------------------
        // Send NACKs requesting the missing chunks.
        // ------------------------------------------------------
        send_nacks(
            sockfd,
            their_addr,
            addr_len,
            missing,
            next_nack_id
        );

        //printf("receiver: sent NACKs for %zu missing chunks.\n", missing.size());

        // -------------------------------------------------------------
        // Wait for retransmitted DATA packets.
        // -------------------------------------------------------------
        //printf("receiver: waiting for retransmitted chunks...\n");
        auto recovery_wait_start = std::chrono::steady_clock::now();
        long chunks_before_retransmission = unique_chunks_received;
        bool first_retransmission = true;
        while(true) {
            addr_len = sizeof(their_addr);

            ssize_t numbytes = recvfrom(
                sockfd,
                packet.data(),
                packet.size(),
                0,
                (struct sockaddr *)&their_addr,
                &addr_len
            );
            if(numbytes == -1) {
               // printf("receiver: timed out waiting for retransmissions.\n");
                auto recovery_wait_end = std::chrono::steady_clock::now();
                double recovery_wait_time = std::chrono::duration<double>(recovery_wait_end - recovery_wait_start).count();
                printf("receiver: recovery wait ended after %.4f sec\n", recovery_wait_time);

                break;
            }
            //Sender finished retransmitting this NACK batch
            // if(numbytes == 1 && (uint8_t)packet[0] == TYPE_RECOVERY_DONE) {
            //     break;
            // }
            if(first_retransmission) {
                auto first_retransmission_time = std::chrono::steady_clock::now();
                double first_delay = std::chrono::duration<double>(first_retransmission_time - recovery_wait_start).count();
                printf("receiver: first retransmission arrived after %.4f sec\n", first_delay);
                first_retransmission = false;
            }


            if((size_t)numbytes < HEADER_SIZE) {
                continue;
            }
            uint8_t type = packet[0];
            if (type != TYPE_DATA && type != TYPE_PARITY) {
                continue;
            }

            uint32_t seq_or_gid;
            uint32_t tchunks;
            uint64_t fsize;

            memcpy(&seq_or_gid, packet.data() + 1, sizeof(seq_or_gid));
            memcpy(&tchunks, packet.data() + 1 + 4, sizeof(tchunks));
            memcpy(&fsize, packet.data() + 1 + 4 + 4, sizeof(fsize));

            if (type == TYPE_DATA) {
                uint32_t seq = seq_or_gid;
                if (seq < total_chunks && !received[seq]) {
                    size_t payload_len = numbytes - HEADER_SIZE;
                    fseek(out, (long)seq * chunk_size, SEEK_SET);
                    fwrite(packet.data() + HEADER_SIZE, 1, payload_len, out);
                    received[seq] = true;
                    unique_chunks_received++;
                    //printf("receiver: received transmitted chunk %u (%ld/%u).\n", seq, unique_chunks_received, total_chunks);
                    end_time = std::chrono::steady_clock::now();

                    //Check whether this retransmission completed the file
                    if(unique_chunks_received == (long)total_chunks){
                        printf("receiver: all %u chunks received.\n", total_chunks);
                        send_done(sockfd, their_addr, addr_len);
                        printf("receiver: sent DONE to sender.\n");
                        break;
                    }
                }
            }
        }
        long received_this_round = unique_chunks_received - chunks_before_retransmission;
        printf("receiver: received %ld retransmitted chunks this round\n", received_this_round);

       // printf("receiver: recovery round took %.3f seconds\n", round_time);
    }

    unrecoverable = (long)total_chunks - unique_chunks_received;
    double elapsed_sec = std::chrono::duration<double>(end_time - start_time).count();
    double mbps = elapsed_sec > 0
        ? ((unique_chunks_received * (double)chunk_size * 8.0) / 1000000.0) / elapsed_sec
        : 0;

    printf("receiver: packets received: %ld (data=%ld, parity=%ld)\n",
           total_packets_received, data_packets, parity_packets);
    printf("receiver: chunks received directly: %ld / %u\n",
           unique_chunks_received - recovered, total_chunks);
    printf("receiver: chunks recovered via XOR: %ld\n", recovered);
    printf("receiver: chunks still missing (unrecoverable): %ld\n", unrecoverable);
    printf("receiver: recovery round: %d\n", recovery_round);
    printf("receiver: elapsed time (first byte -> last unique byte): %.4f sec\n", elapsed_sec);
    printf("receiver: throughput (unique payload): %.2f Mbps\n", mbps);

    if (unrecoverable > 0) {
        fprintf(stderr, "receiver: WARNING -- %ld chunk(s) still missing, file is incomplete.\n",
                unrecoverable);
    }

    fclose(out);
    close(sockfd);
    return 0;
}