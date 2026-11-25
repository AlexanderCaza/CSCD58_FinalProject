#pragma once

#include "lqtt_broker.h"
#include "packet.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// Max number of messages that can be inflight at once: 8 * max_clients
#define BROKER_MAX_INFLIGHT (LQTT_MAX_CLIENTS * 8)
#define BROKER_BASE_RETRY_TICKS 20 // 20 ticks between retransmisions
#define BROKER_MAX_JITTER_TICKS 15 // 15 ticks of jitter for retransmission attempts
#define BROKER_MAX_RETRIES 5 // Max retries per un-acked message before giving up

// Represents an in-flight QoS 1 message
typedef struct {
    bool active; // Is this slot tracking an inflight packet?
    uint16_t client_id; // What client_id is this message for?
    uint8_t topic_id; 
    uint8_t msg_id;
    uint8_t retries;
    uint16_t ticks_remaining; // before next re-transmission attempt
    lqtt_packet_t packet;
} broker_inflight_t; 

typedef struct {
    lqtt_broker_node_t node;
    int sock;

    broker_inflight_t inflight[BROKER_MAX_INFLIGHT];
    uint8_t last_received_msg_id[LQTT_MAX_TOPICS][LQTT_MAX_CLIENTS]; // Last received QoS 1 msg_id per (topic, client) for duplicate suppression
    bool has_last_received[LQTT_MAX_TOPICS][LQTT_MAX_CLIENTS]; // Whether there is a last received message for this (topic, client)
} broker_controller_t;

// Initializes the broker controller, its internal lqtt_broker_node_t, and socket binding.
// Returns: true if successful, false on fail
bool broker_controller_init(broker_controller_t *controller, int sock);

// Processes a raw LQTT packet, validates checksum/length,
// handles subs and unsubs, updates retain ring, sends ACKs, and publishes messages to clients.
void broker_process_packet(broker_controller_t *controller, const uint8_t *buf, size_t len);

// Advances QoS 1 retransmission counters by 1 tick and transmits any retained messages to 
// behind clients
void broker_controller_tick(broker_controller_t *controller);