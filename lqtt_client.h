#pragma once

#include "packet.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#define BROKER_ID 0x0000

typedef struct {
    uint16_t node_id; // the ID of this node
    uint8_t subbed_topics[256]; // [topicId] = 1 if subbed with QoS 0, = 2 if subbed with QoS 1, 
    // not subbed otherwise
    uint8_t sent_packet_id; // The ID of the last outbound message
} lqtt_client_node_t;

// Initializes the lqtt client node in client_node
bool lqtt_client_init(uint16_t node_id, lqtt_client_node_t *client_node);

// Creates a subscribe packet for a given topic. client_node is the sending node
// topic_id is the topic to sub to, qos is desired quality of service
// packet is populated with the packet
// Returns: true if successful, false on fail
bool create_subscribe_packet(lqtt_client_node_t *client_node, 
    uint8_t topic_id, uint8_t qos, lqtt_packet_t *packet);

// Creates an unsubscribe packet for a given topic. client_node is the sending node
// topic_id is the topic to unsub from.
// packet is populated with the packet
// Returns: true if successful, false on fail
bool create_unsubscribe_packet(lqtt_client_node_t *client_node, 
    uint8_t topic_id, lqtt_packet_t *packet);

// Creates a publish packet for a given topic containing a payload. client_node is the sending node
// topic_id is the topic the payload is for. qos is the qos option. Retain is whether the message should
// be retained by the broker. dupe indicates whether a message is a retransmission.
// payload is the payload to send and payload_length is the length of said payload
// packet is populated with the packet
// Returns: true if successful, false on fail
bool create_publish_packet(lqtt_client_node_t *client_node, uint8_t topic_id, bool qos, 
    bool retain, bool dupe, const void *payload, size_t payload_length, lqtt_packet_t *packet);

// Creates a publish acknowledgment packet for a received QoS 1 publish message. client_node is the sending node
// topic_id is the topic of the received publish. msg_id is the message ID from the received publish packet.
// packet is populated with the packet
// Returns: true if successful, false on fail
bool create_puback_packet(lqtt_client_node_t *client_node, uint8_t topic_id, 
    uint8_t msg_id, lqtt_packet_t *packet);
