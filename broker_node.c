#include "broker_controller.h"
#include "lqtt_broker.h"
#include "raw_ethernet.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define TICK_MS 10

static broker_controller_t broker_controller;

// Returns current monotonic time in milliseconds
static int64_t now_ms_monotonic(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + (int64_t)(ts.tv_nsec / 1000000);
}

// Parses command-line arguments into individual variables
static int process_input(int argument_count, char **arguments, const char **iface, int *tick_ms) {
    if (argument_count < 2) {
        fprintf(stderr, "Usage: %s <iface> [--tick=<ms>]\n", arguments[0]);
        return 1;
    }

    *iface = arguments[1];
    *tick_ms = TICK_MS;

    for (int i = 2; i < argument_count; i++) {
        char *argument = arguments[i];
        if (strstr(argument, "--tick")) {
            char dest[100];
            strncpy(dest, argument + 7, sizeof(dest) - 1);
            dest[sizeof(dest) - 1] = '\0';
            int val = atoi(dest);
            if (val > 0) {
                *tick_ms = val;
            }
        } else {
            printf("Unrecognized option %s\n", argument);
            return 1;
        }
    }
    return 0;
}

// Processes broker CLI commands from stdin
static void handle_command(char *line) {
    line[strcspn(line, "\r\n")] = '\0';
    if (line[0] == '\0') return;

    // Skip leading whitespace
    char *p = line;
    while (*p == ' ' || *p == '\t') p++;
    if (*p == '\0') return;

    if (strcmp(p, "status") == 0) {
        printf("Broker 0x%04X active\n", BROKER_ID);
    } else {
        printf("Unrecognized command %s\n", p);
    }
    fflush(stdout);
}

int main(int argc, char **argv) {
    const char *iface = NULL;
    int tick_ms = TICK_MS;

    if (process_input(argc, argv, &iface, &tick_ms)) {
        return 1;
    }

    int sock = raw_open(iface);
    if (sock < 0) {
        perror("raw_open");
        return 1;
    }

    // Prevent raw_recv from blocking if poll wakes on filtered frames
    int flags = fcntl(sock, F_GETFL, 0);
    if (flags < 0 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("fcntl(O_NONBLOCK)");
        close(sock);
        return 1;
    }

    if (!broker_controller_init(&broker_controller, sock, BROKER_ID)) {
        fprintf(stderr, "Failed to init broker_controller\n");
        close(sock);
        return 1;
    }

    printf("Broker Id: 0x%04X on %s\n", BROKER_ID, iface);
    printf("Commands: status\n");
    fflush(stdout);

    struct pollfd fds[2] = {
        {STDIN_FILENO, POLLIN, 0},
        {sock, POLLIN, 0},
    };
    char line[256];
    int64_t next_tick_ms = now_ms_monotonic() + tick_ms;

    while (1) {
        // Compute remaining time until the next controller tick
        int64_t now_ms = now_ms_monotonic();
        int timeout_ms = (next_tick_ms > now_ms) ? (int)(next_tick_ms - now_ms) : 0;

        int ready = poll(fds, 2, timeout_ms);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("poll");
            break;
        }

        if (ready > 0) {
            // Handle incoming raw Ethernet frames
            if (fds[1].revents & POLLIN) {
                uint8_t buf[MAX_PACKET_LENGTH];
                int n = raw_recv(sock, buf, sizeof(buf));
                if (n > 0) {
                    print_received(buf, n);
                    broker_process_packet(&broker_controller, buf, (size_t)n);
                }
            }
            // Handle interactive CLI input from stdin
            if (fds[0].revents & POLLIN) {
                if (fgets(line, sizeof(line), stdin)) {
                    handle_command(line);
                } else {
                    break;
                }
            }
        }

        // Catch up ticks if poll returned late or multiple tick windows passed
        now_ms = now_ms_monotonic();
        while (now_ms >= next_tick_ms) {
            broker_controller_tick(&broker_controller);
            next_tick_ms += tick_ms;
        }
    }

    close(sock);
    return 0;
}