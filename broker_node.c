#include "raw_ethernet.h"

#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define BROKER_ID 0x0000

static void print_received(const uint8_t *buf, int len) {
    printf("\n[Broker recieved: %dB]: ", len);
    for (int i = 0; i < len; i++) {
        for (int b = 7; b >= 0; b--) putchar((buf[i] & (1 << b)) ? '1' : '0');
        putchar(' ');
    }
    printf("(\"%.*s\")\n", len, buf);
    fflush(stdout);
}

int main(int argc, char *argv[]) {
    if (argc < 2) return fprintf(stderr, "Usage: %s <iface>\n", argv[0]), 1;

    int sock = raw_open(argv[1]);
    if (sock < 0) return perror("raw_open"), 1;

    struct pollfd fds[2] = {{STDIN_FILENO, POLLIN, 0}, {sock, POLLIN, 0}};
    char line[256];
    uint8_t buf[MAX_PACKET_LENGTH];

    printf("[Broker 0x%04X on %s] Type: send <msg>\n", BROKER_ID, argv[1]);
    fflush(stdout);

    while (poll(fds, 2, -1) > 0) {
        if (fds[1].revents & POLLIN) {
            int n = raw_recv(sock, buf, sizeof(buf));
            if (n > 0) print_received(buf, n);
        }
        if (fds[0].revents & POLLIN && fgets(line, sizeof(line), stdin)) {
            line[strcspn(line, "\r\n")] = '\0';
            if (strncmp(line, "send ", 5) == 0 && line[5] != '\0') {
                size_t len = strlen(line + 5);
                if (len > MAX_PACKET_LENGTH) len = MAX_PACKET_LENGTH;
                raw_send(sock, BROKER_ID, (uint8_t *)(line + 5), len);
            }
            fflush(stdout);
        }
    }
    close(sock);
    return 0;
}