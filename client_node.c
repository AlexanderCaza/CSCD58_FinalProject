#include "client_controller.h"
#include "lqtt_client.h"
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

static client_controller_t client_controller;

// Returns current monotonic time in milliseconds
static int64_t now_ms_monotonic(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000LL + (int64_t)(ts.tv_nsec / 1000000);
}

// Sets a file descriptor to non-blocking mode
static bool set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

// Parses command-line arguments into individual variables
static int process_input(int argument_count, char **arguments, const char **iface,
                         uint16_t *node_id, int *tick_ms) {
    if (argument_count < 3) {
        fprintf(stderr, "Usage: %s <iface> <nodeId> [--tick=<ms>]\n", arguments[0]);
        return 1;
    }

    *iface = arguments[1];
    *node_id = (uint16_t)atoi(arguments[2]);
    *tick_ms = TICK_MS;

    for (int i = 3; i < argument_count; i++) {
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

// On message received, we output the message
static void on_message_received(uint8_t topic_id, const uint8_t *payload, uint8_t payload_len) {
    printf("Topic %" PRIu8 ": %.*s\n", topic_id, (int)payload_len, (const char *)payload);
    fflush(stdout);
}

// Handles subscribing to a topic
static void handle_sub(uint8_t topic_id, bool qos1) {
    if (!client_subscribe(&client_controller, topic_id, qos1)) {
        printf("Failed to subscribe to topic %" PRIu8 "\n", topic_id);
        return;
    }
    printf("Subscribed to topic %" PRIu8 " (qos=%d)\n", topic_id, qos1 ? 1 : 0);
}

// Handles unsubscribing from a topic
static void handle_unsub(uint8_t topic_id) {
    if (!client_unsubscribe(&client_controller, topic_id)) {
        printf("Failed to unsubscribe from topic %" PRIu8 "\n", topic_id);
        return;
    }
    printf("Unsubscribed from topic %" PRIu8 "\n", topic_id);
}

// Handles publishing a message to a topic
static void handle_publish(uint8_t topic_id, bool qos1, bool retain, const char *msg) {
    size_t len = strlen(msg);
    if (len > LQTT_MAX_PAYLOAD_LEN) {
        len = LQTT_MAX_PAYLOAD_LEN;
    }

    if (!client_publish(&client_controller, topic_id, qos1, retain, (const uint8_t *)msg, (uint8_t)len)) {
        printf("Failed to publish message on topic %" PRIu8 "\n", topic_id);
        return;
    }
    printf("Published to topic %" PRIu8 " (qos=%d, retain=%d): %.*s\n",
           topic_id, qos1 ? 1 : 0, retain ? 1 : 0, (int)len, msg);
}

// Processes user input
static void handle_command(char *line) {
    line[strcspn(line, "\r\n")] = '\0';
    if (line[0] == '\0') return;

    // Skip leading whitespace
    char *p = line;
    while (*p == ' ' || *p == '\t') p++;
    if (*p == '\0') return;

    if (!(strncmp(p, "sub ", 4)) || !(strcmp(p, "sub"))) {
        char *args = p[3] ? p + 4 : p + 3;
        while (*args == ' ' || *args == '\t') args++;
        if (*args == '\0') {
            printf("Usage: sub [-qos=0|1] <topicId>\n");
            fflush(stdout);
            return;
        }

        // Parse optional leading -qos=0|1 flag
        bool qos1 = false;
        if (strncmp(args, "-qos=", 5) == 0) {
            char val[16];
            strncpy(val, args + 5, sizeof(val) - 1);
            val[sizeof(val) - 1] = '\0';
            char *next_space = strpbrk(val, " \t");
            if (next_space) *next_space = '\0';

            if (!(strcmp(val, "0"))) {
                qos1 = false;
            } else if (!(strcmp(val, "1"))) {
                qos1 = true;
            } else {
                printf("Unrecognized qos option %s\n", val);
                fflush(stdout);
                return;
            }

            args += 5;
            while (*args && *args != ' ' && *args != '\t') args++;
            while (*args == ' ' || *args == '\t') args++;
        }

        if (*args == '\0') {
            printf("Usage: sub [-qos=0|1] <topicId>\n");
            fflush(stdout);
            return;
        }

        int topic_val = atoi(args);
        if (topic_val < 0 || topic_val > UINT8_MAX) {
            printf("Invalid topicId %s\n", args);
            fflush(stdout);
            return;
        }

        handle_sub((uint8_t)topic_val, qos1);
    } else if (!(strncmp(p, "unsub ", 6)) || !(strcmp(p, "unsub"))) {
        char *args = p[5] ? p + 6 : p + 5;
        while (*args == ' ' || *args == '\t') args++;
        if (*args == '\0') {
            printf("Usage: unsub <topicId>\n");
            fflush(stdout);
            return;
        }

        int topic_val = atoi(args);
        if (topic_val < 0 || topic_val > UINT8_MAX) {
            printf("Invalid topicId %s\n", args);
            fflush(stdout);
            return;
        }

        handle_unsub((uint8_t)topic_val);
    } else if (!(strncmp(p, "publish ", 8)) || !(strcmp(p, "publish"))) {
        char *args = p[7] ? p + 8 : p + 7;
        while (*args == ' ' || *args == '\t') args++;
        if (*args == '\0') {
            printf("Usage: publish [-qos=0|1] [-retain=0|1] <topicId> <message>\n");
            fflush(stdout);
            return;
        }

        // Parse optional leading -qos=0|1 flag
        bool qos1 = false;
        if (strncmp(args, "-qos=", 5) == 0) {
            char val[16];
            strncpy(val, args + 5, sizeof(val) - 1);
            val[sizeof(val) - 1] = '\0';
            char *next_space = strpbrk(val, " \t");
            if (next_space) *next_space = '\0';

            if (!(strcmp(val, "0"))) {
                qos1 = false;
            } else if (!(strcmp(val, "1"))) {
                qos1 = true;
            } else {
                printf("Unrecognized qos option %s\n", val);
                fflush(stdout);
                return;
            }

            args += 5;
            while (*args && *args != ' ' && *args != '\t') args++;
            while (*args == ' ' || *args == '\t') args++;
        }

        // Parse optional -retain=0|1 flag immediately following command or -qos=
        bool retain = false;
        if (strncmp(args, "-retain=", 8) == 0) {
            char val[16];
            strncpy(val, args + 8, sizeof(val) - 1);
            val[sizeof(val) - 1] = '\0';
            char *next_space = strpbrk(val, " \t");
            if (next_space) *next_space = '\0';

            if (!(strcmp(val, "0"))) {
                retain = false;
            } else if (!(strcmp(val, "1"))) {
                retain = true;
            } else {
                printf("Unrecognized retain option %s\n", val);
                fflush(stdout);
                return;
            }

            args += 8;
            while (*args && *args != ' ' && *args != '\t') args++;
            while (*args == ' ' || *args == '\t') args++;
        }

        // Extract topicId before the message payload
        char *next_space = strpbrk(args, " \t");
        if (!next_space) {
            printf("Usage: publish [-qos=0|1] [-retain=0|1] <topicId> <message>\n");
            fflush(stdout);
            return;
        }
        *next_space = '\0';

        int topic_val = atoi(args);
        if (topic_val < 0 || topic_val > UINT8_MAX) {
            printf("Invalid topicId %s\n", args);
            fflush(stdout);
            return;
        }

        // Extract message text after topicId
        char *msg = next_space + 1;
        while (*msg == ' ' || *msg == '\t') msg++;
        if (*msg == '\0') {
            printf("Usage: publish [-qos=0|1] [-retain=0|1] <topicId> <message>\n");
            fflush(stdout);
            return;
        }

        handle_publish((uint8_t)topic_val, qos1, retain, msg);
    } else {
        printf("Unrecognized option %s\n", p);
    }
    fflush(stdout);
}

int main(int argc, char **argv) {
    const char *iface = NULL;
    uint16_t node_id = 0;
    int tick_ms = TICK_MS;

    if (process_input(argc, argv, &iface, &node_id, &tick_ms)) {
        return 1;
    }

    int sock = raw_open(iface);
    if (sock < 0) {
        perror("raw_open");
        return 1;
    }

    // Prevent raw_recv from blocking if poll wakes on filtered frames
    if (!set_nonblocking(sock)) {
        perror("fcntl(O_NONBLOCK)");
        close(sock);
        return 1;
    }

    if (!client_controller_init(&client_controller, sock, node_id, on_message_received)) {
        fprintf(stderr, "Failed to init client_controller\n");
        close(sock);
        return 1;
    }

    printf("Node Id: %" PRIu16 "\n", node_id);
    printf("Commands: sub [-qos=0|1] <topicId> | publish [-qos=0|1] [-retain=0|1] <topicId> <msg> | unsub <topicId>\n");
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
                    client_process_packet(&client_controller, buf, (size_t)n);
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
            client_controller_tick(&client_controller);
            next_tick_ms += tick_ms;
        }
    }

    close(sock);
    return 0;
}