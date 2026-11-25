#pragma once

#include "packet.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#define LQTT_SLOT_EMPTY 0xFF
#define LQTT_RETAIN_CAUGHT_UP 0xFF

#define BROKER_ID 0x0000

// Define limits that tradeoff capacity for memory efficiency
#define LQTT_MAX_CLIENTS 256 // including self (1 wasted slot to simplify design)
#define LQTT_MAX_TOPICS 256 
#define LQTT_MAX_RETAINED_MESSAGES 10

typedef struct {
    uint8_t payload[LQTT_MAX_PAYLOAD_LEN];
    uint8_t payload_length; // 0 to 120 = valid payload length, 0xFF = empty slot
} lqtt_retained_msg_t;

typedef struct {
    uint16_t node_id; // Always BROKER_ID (0x0000)
    // [topic_id][node_id]: 0 = unsubbed, 1 = QoS 0, 2 = QoS 1
    uint8_t subs[LQTT_MAX_TOPICS][LQTT_MAX_CLIENTS];
    // Circular ring buffer of last 10 retained messages per topic
    lqtt_retained_msg_t retained[LQTT_MAX_TOPICS][LQTT_MAX_RETAINED_MESSAGES];
    uint8_t retain_head[LQTT_MAX_TOPICS]; // Next write index in retained[topic_id] (0..9)
    // Next ring index to send to client, or LQTT_RETAIN_CAUGHT_UP
    uint8_t client_retain_idx[LQTT_MAX_TOPICS][LQTT_MAX_CLIENTS];
    uint8_t sent_packet_id; // The ID of the last outbound broker publish
} lqtt_broker_node_t;

// Initializes the lqtt broker node in broker_node
bool lqtt_broker_init(lqtt_broker_node_t *broker_node);

// Creates a subscribe acknowledgment packet for a received subscribe message.
// broker_node is the sending node, client_id is the target client node ID,
// topic_id is the subscribed topic, and msg_id is the message ID from the subscribe packet.
// packet is populated with the packet.
// Returns: true if successful, false on fail
bool create_suback_packet(lqtt_broker_node_t *broker_node, uint16_t client_id, 
    uint8_t topic_id, uint8_t msg_id, lqtt_packet_t *packet);

// Creates an unsubscribe acknowledgment packet for a received unsubscribe message.
// broker_node is the sending node, client_id is the target client node ID,
// topic_id is the unsubscribed topic, and msg_id is the message ID from the unsubscribe packet.
// packet is populated with the packet.
// Returns: true if successful, false on fail
bool create_unsuback_packet(lqtt_broker_node_t *broker_node, uint16_t client_id, 
    uint8_t topic_id, uint8_t msg_id, lqtt_packet_t *packet);

// Creates a publish packet forwarded from the broker to a subscribed client.
// broker_node is the sending node, client_id is the target client node ID,
// topic_id is the topic the payload is for, and qos is the subscriber's QoS option.
// dupe indicates whether a message is a retransmission.
// payload is the payload to send, payload_length is its length, and packet is populated.
// Returns: true if successful, false on fail
bool create_broker_publish_packet(lqtt_broker_node_t *broker_node, uint16_t client_id,
    uint8_t topic_id, bool qos, bool dupe, const void *payload, size_t payload_length,
    lqtt_packet_t *packet);

// Creates a publish acknowledgment packet for a received QoS 1 publish message.
// broker_node is the sending node, client_id is the target client node ID,
// topic_id is the topic of the received publish, and msg_id is the received publish message ID.
// packet is populated with the packet.
// Returns: true if successful, false on fail
bool create_broker_puback_packet(lqtt_broker_node_t *broker_node, uint16_t client_id, 
    uint8_t topic_id, uint8_t msg_id, lqtt_packet_t *packet);