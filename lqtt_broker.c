#include "lqtt_broker.h"
#include "packet.h"

#include <stdbool.h>
#include <string.h>
#include <arpa/inet.h>

bool lqtt_broker_init(lqtt_broker_node_t *broker_node) {
    // Sanity checking
    if (!broker_node) {
        return false;
    }
    // Clear any junk
    memset(broker_node, 0, sizeof(*broker_node));
    // Set the node ID
    broker_node->node_id = BROKER_ID;
    // Mark all retained message slots as empty and client cursors as caught up
    memset(broker_node->retained, LQTT_SLOT_EMPTY, sizeof(broker_node->retained));
    memset(broker_node->client_retain_idx, LQTT_RETAIN_CAUGHT_UP, sizeof(broker_node->client_retain_idx));
    return true;
}

bool create_suback_packet(lqtt_broker_node_t *broker_node, uint16_t client_id, 
    uint8_t topic_id, uint8_t msg_id, lqtt_packet_t *packet) {
    if (!broker_node || !packet || client_id == 0 || client_id >= LQTT_MAX_CLIENTS) {
        return false;
    }

    // Clear junk from the packet
    memset(packet, 0, sizeof(*packet));
    // Populate the packet
    packet->node_id = htons(client_id);
    packet->topic_id = topic_id;
    packet->length = LQTT_HEADER_LEN;
    packet->msg_type = LQTT_MSG_SUBSCRIBE_ACK;
    packet->msg_id = msg_id;
    // Compute and add the checksum
    packet->checksum = compute_checksum(packet);

    return true;
}

bool create_unsuback_packet(lqtt_broker_node_t *broker_node, uint16_t client_id, 
    uint8_t topic_id, uint8_t msg_id, lqtt_packet_t *packet) {
    if (!broker_node || !packet || client_id == 0 || client_id >= LQTT_MAX_CLIENTS) {
        return false;
    }

    // Clear junk from the packet
    memset(packet, 0, sizeof(*packet));
    // Populate the packet
    packet->node_id = htons(client_id);
    packet->topic_id = topic_id;
    packet->length = LQTT_HEADER_LEN;
    packet->msg_type = LQTT_MSG_UNSUBSCRIBE_ACK;
    packet->msg_id = msg_id;
    // Compute and add the checksum
    packet->checksum = compute_checksum(packet);

    return true;
}

bool create_broker_publish_packet(lqtt_broker_node_t *broker_node, uint16_t client_id,
    uint8_t topic_id, bool qos, bool dupe, const void *payload, size_t payload_length,
    lqtt_packet_t *packet) {
    // Sanity checks
    if (!broker_node || !packet || client_id == BROKER_ID || client_id >= LQTT_MAX_CLIENTS ||
        payload_length > LQTT_MAX_PAYLOAD_LEN || (!payload && payload_length > 0)) {
        return false;
    }

    // Get a new sent_packet_id
    if (!dupe) {
        broker_node->sent_packet_id++;
    }

    // Clear junk from the packet
    memset(packet, 0, sizeof(*packet));
    // Populate the packet
    packet->node_id = htons(client_id);
    packet->topic_id = topic_id;
    if (qos) {
        packet->flags |= LQTT_FLAG_QOS1;
    }
    if (dupe) {
        packet->flags |= LQTT_FLAG_DUPE;
    }
    packet->length = LQTT_HEADER_LEN + payload_length;
    packet->msg_type = LQTT_MSG_PUBLISH;
    packet->msg_id = broker_node->sent_packet_id;
    if (payload && payload_length > 0) {
        memcpy(packet->payload, payload, payload_length);
    }
    // Compute and add the checksum
    packet->checksum = compute_checksum(packet);

    return true;
}

bool create_broker_puback_packet(lqtt_broker_node_t *broker_node, uint16_t client_id, 
    uint8_t topic_id, uint8_t msg_id, lqtt_packet_t *packet) {
    if (!broker_node || !packet || client_id == BROKER_ID || client_id >= LQTT_MAX_CLIENTS) {
        return false;
    }

    // Clear junk from the packet
    memset(packet, 0, sizeof(*packet));
    // Populate the packet
    packet->node_id = htons(client_id);
    packet->topic_id = topic_id;
    packet->length = LQTT_HEADER_LEN;
    packet->msg_type = LQTT_MSG_PUBLISH_ACK;
    packet->msg_id = msg_id;
    // Compute and add the checksum
    packet->checksum = compute_checksum(packet);

    return true;
}