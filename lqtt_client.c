#include "lqtt_client.h"
#include "packet.h"

#include <stdbool.h>
#include <string.h>
#include <arpa/inet.h>

bool lqtt_client_init(uint16_t node_id, lqtt_client_node_t *client_node) {
    // Sanity checking
    if (!client_node || node_id == BROKER_ID) {
        return false;
    }
    // Clear any junk
    memset(client_node, 0, sizeof(*client_node));
    // Set the node ID
    client_node->node_id = node_id;
    return true;
}


bool create_subscribe_packet(lqtt_client_node_t *client_node, 
    uint8_t topic_id, uint8_t qos, lqtt_packet_t *packet) {
    if (!client_node || !packet || qos > 1) {
        return false;
    }

    // Get a new sent_packet_id
    client_node->sent_packet_id++;

    // Clear junk from the packet
    memset(packet, 0, sizeof(*packet));
    // Populate the packet
    packet->node_id = htons(client_node->node_id);
    packet->topic_id = topic_id;
    if (qos == 1) {
        packet->flags = LQTT_FLAG_QOS1;
    }
    packet->length = LQTT_HEADER_LEN;
    packet->msg_type = LQTT_MSG_SUBSCRIBE;
    packet->msg_id = client_node->sent_packet_id;
    // Compute and add the checksum
    packet->checksum = compute_checksum(packet);

    // Update client node
    client_node->subbed_topics[topic_id] = qos + 1; // 1 = QoS 0, 2 = QoS 1

    return true;
}

bool create_unsubscribe_packet(lqtt_client_node_t *client_node, 
    uint8_t topic_id, lqtt_packet_t *packet) {
    if (!client_node || !packet) {
        return false;
    }

    // Get a new sent_packet_id
    client_node->sent_packet_id++;

    // Clear junk from the packet
    memset(packet, 0, sizeof(*packet));
    // Populate the packet
    packet->node_id = htons(client_node->node_id);
    packet->topic_id = topic_id;
    packet->length = LQTT_HEADER_LEN;
    packet->msg_type = LQTT_MSG_UNSUBSCRIBE;
    packet->msg_id = client_node->sent_packet_id;
    // Compute and add the checksum
    packet->checksum = compute_checksum(packet);

    // Update client node
    client_node->subbed_topics[topic_id] = 0;

    return true;
}

bool create_publish_packet(lqtt_client_node_t *client_node, uint8_t topic_id, bool qos, 
    bool retain, bool dupe, const void *payload, size_t payload_length, lqtt_packet_t *packet) {
    // Sanity checks
    if (!client_node || !packet || payload_length > LQTT_MAX_PAYLOAD_LEN || 
        (!payload && payload_length > 0)) {
        return false;
    }

    // Get a new sent_packet_id
    if (!dupe) {
        client_node->sent_packet_id++;
    }

    // Clear junk from the packet
    memset(packet, 0, sizeof(*packet));
    // Populate the packet
    packet->node_id = htons(client_node->node_id);
    packet->topic_id = topic_id;
    if (qos) {
        packet->flags |= LQTT_FLAG_QOS1;
    }
    if (retain) {
        packet->flags |= LQTT_FLAG_RETAIN;
    }
    if (dupe) {
        packet->flags |= LQTT_FLAG_DUPE;
    }
    packet->length = LQTT_HEADER_LEN + payload_length;
    packet->msg_type = LQTT_MSG_PUBLISH;
    packet->msg_id = client_node->sent_packet_id;
    if (payload && payload_length > 0) {
        memcpy(packet->payload, payload, payload_length);
    }
    // Compute and add the checksum
    packet->checksum = compute_checksum(packet);

    return true;
}

bool create_puback_packet(lqtt_client_node_t *client_node, uint8_t topic_id, 
    uint8_t msg_id, lqtt_packet_t *packet) {
    if (!client_node || !packet) {
        return false;
    }

    // Clear junk from the packet
    memset(packet, 0, sizeof(*packet));
    // Populate the packet
    packet->node_id = htons(client_node->node_id);
    packet->topic_id = topic_id;
    packet->length = LQTT_HEADER_LEN;
    packet->msg_type = LQTT_MSG_PUBLISH_ACK;
    packet->msg_id = msg_id;
    // Compute and add the checksum
    packet->checksum = compute_checksum(packet);

    return true;
}