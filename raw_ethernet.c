#include "raw_ethernet.h"

#include <arpa/inet.h>
#include <linux/if_packet.h>
#include <net/ethernet.h>
#include <net/if.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct __attribute__((packed)) {
    struct ethhdr eth;
    uint8_t payload[MAX_PACKET_LENGTH];
} raw_frame_t;

int raw_open(const char *ifname) {
    int sock = socket(AF_PACKET, SOCK_RAW, htons(CUSTOM_ETHERTYPE));
    if (sock < 0) return -1;

    struct sockaddr_ll sll = {
        .sll_family   = AF_PACKET,
        .sll_protocol = htons(CUSTOM_ETHERTYPE),
        .sll_ifindex  = if_nametoindex(ifname)
    };

    if (sll.sll_ifindex == 0 || bind(sock, (struct sockaddr *)&sll, sizeof(sll)) < 0) {
        close(sock);
        return -1;
    }
    return sock;
}

int raw_send(int sock, uint16_t node_id, const uint8_t *payload, size_t len) {
    if (len > MAX_PACKET_LENGTH) return -1;

    raw_frame_t frame = {
        .eth = {
            .h_dest   = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
            .h_source = {0x02, 0x00, 0x00, 0x00, node_id >> 8, node_id & 0xFF},
            .h_proto  = htons(CUSTOM_ETHERTYPE)
        }
    };
    memcpy(frame.payload, payload, len);

    ssize_t sent = send(sock, &frame, sizeof(frame.eth) + len, 0);
    return (sent < 0) ? -1 : (int)(sent - sizeof(frame.eth));
}

int raw_recv(int sock, uint8_t *out_buf, size_t max_len) {
    raw_frame_t frame;
    struct sockaddr_ll sll;
    socklen_t sll_len = sizeof(sll);

    while (1) {
        ssize_t n = recvfrom(sock, &frame, sizeof(frame), 0,
                             (struct sockaddr *)&sll, &sll_len);
        if (n < 0) return -1;
        // Ignore our own transmissions
        if (sll.sll_pkttype == PACKET_OUTGOING) continue;
        if (n <= (ssize_t)sizeof(frame.eth)) continue;

        size_t len = (size_t)n - sizeof(frame.eth);
        if (len > max_len) len = max_len;

        memcpy(out_buf, frame.payload, len);
        return (int)len;
    }
}

