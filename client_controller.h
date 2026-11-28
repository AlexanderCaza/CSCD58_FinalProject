#pragma once

#include "packet.h"
#include "lqtt_client.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define CLIENT_MAX_TOPICS 256
#define CLIENT_MAX_INFLIGHT 32
#define CLIENT_BASE_RETRY_TICKS 20 // 20 ticks between retransmissions
#define CLIENT_MAX_JITTER_TICKS 15 // 15 ticks of jitter for retransmission attempts
#define CLIENT_MAX_RETRIES 5 // Max retries per un-acked message before giving up

// Callback invoked when a valid, non-duplicate PUBLISH arrives from the broker
typedef void (*client_message_callback_t)(uint8_t topic_id, const uint8_t *payload,
                                    uint8_t payload_len);

// Represents an in-flight packet awaiting an ACK (SUBSCRIBE, UNSUBSCRIBE, or QoS 1 PUBLISH)
typedef struct {
    bool active; // Is this slot tracking an inflight packet?
    uint8_t expected_ack; // Expected response type (SUBACK, UNSUBACK, or PUBACK)
    uint8_t topic_id;
    uint8_t msg_id;
    uint8_t retries; // number of retries already
    uint16_t ticks_remaining; // Ticks remaining before next retransmission attempt
    lqtt_packet_t packet;
} client_inflight_t;

// The client controller
typedef struct {
    lqtt_client_node_t node;
    uint16_t client_id;
    int sock;
    uint8_t next_msg_id;
    uint8_t subbed_topics[CLIENT_MAX_TOPICS]; // 0 = unsubscribed, 1 = QoS 0, 2 = QoS 1
    client_inflight_t inflight[CLIENT_MAX_INFLIGHT];
    uint8_t last_received_msg_id[CLIENT_MAX_TOPICS]; // Last received QoS 1 msg_id per topic
    bool has_last_received[CLIENT_MAX_TOPICS]; // Whether a QoS 1 message has been received on this topic
    client_message_callback_t on_message;
} client_controller_t;

// Initializes the client controller, socket binding, node ID, and optional message callback.
// Returns: true if successful, false on fail
bool client_controller_init(client_controller_t *controller, int sock, uint16_t client_id,
                            client_message_callback_t on_message);

// Sends a SUBSCRIBE request for topic_id at the requested QoS level and tracks it until SUBACK.
bool client_subscribe(client_controller_t *controller, uint8_t topic_id, bool qos1);

// Sends an UNSUBSCRIBE request for topic_id and tracks it until UNSUBACK.
bool client_unsubscribe(client_controller_t *controller, uint8_t topic_id);

// Publishes a payload to topic_id (with optional QoS 1 tracking and retain flag).
bool client_publish(client_controller_t *controller, uint8_t topic_id, bool qos1, bool retain,
                    const uint8_t *payload, uint8_t payload_len);

// Processes an incoming raw LQTT packet from the broker, validates checksum/length,
// clears matching inflight entries on ACKs, avoids dupe messages for QoS, and triggers on_message.
void client_process_packet(client_controller_t *controller, const uint8_t *buf, size_t len);

// Advances retransmission counters by 1 tick for un-ACKed SUBSCRIBE, UNSUBSCRIBE, and QoS 1 PUBLISH packets.
void client_controller_tick(client_controller_t *controller);