#include "broker_controller.h"
#include "raw_ethernet.h"

#include <arpa/inet.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

// Generates jitter to reduce collisions on retransmissions
static uint16_t next_jitter_ticks(void) {
    return (uint16_t)(rand() % (BROKER_MAX_JITTER_TICKS + 1));
}

// Send a packet, and if QoS is 1, enter it into our tracking system
static bool send_and_track(broker_controller_t *controller, uint16_t client_id,
                           uint8_t topic_id, bool qos1, const lqtt_packet_t *packet) {
    if (raw_send(controller->sock, BROKER_ID, (const uint8_t *)packet, packet->length) < 0) {
        return false;
    }

    // If not using QoS 1, we don't need to track it
    if (!qos1) {
        return true;
    }

    for (size_t i = 0; i < BROKER_MAX_INFLIGHT; i++) {
        // Find an empty slot in the inflight tracking array and add the packet
        if (!controller->inflight[i].active) {
            controller->inflight[i].active = true;
            controller->inflight[i].client_id = client_id;
            controller->inflight[i].topic_id = topic_id;
            controller->inflight[i].msg_id = packet->msg_id;
            controller->inflight[i].retries = 0;
            controller->inflight[i].ticks_remaining = BROKER_BASE_RETRY_TICKS + next_jitter_ticks();
            memcpy(&controller->inflight[i].packet, packet, sizeof(*packet));
            return true;
        }
    }

    return false;
}

// Remove a packet from our QoS 1 tracking system so we don't retransmit it
static void clear_inflight(broker_controller_t *controller, uint16_t client_id,
                           uint8_t topic_id, uint8_t msg_id) {
    for (size_t i = 0; i < BROKER_MAX_INFLIGHT; i++) {
        if (controller->inflight[i].active &&
            controller->inflight[i].client_id == client_id &&
            controller->inflight[i].topic_id == topic_id &&
            controller->inflight[i].msg_id == msg_id) {
            controller->inflight[i].active = false;
            return;
        }
    }
}

// Store a payload in our retain ring, bumping any lagging client cursors on overwrite
static void store_retained(lqtt_broker_node_t *node, uint8_t topic_id,
                           const uint8_t *payload, uint8_t payload_len) {
    uint8_t slot_index = node->retain_head[topic_id];
    uint8_t next_head = (slot_index + 1) % LQTT_MAX_RETAINED_MESSAGES;

    // If overwriting an occupied slot, advance any client cursor sitting on it
    // so the client drops only the overwritten message and still gets the 10 newest.
    if (node->retained[topic_id][slot_index].payload_length != LQTT_SLOT_EMPTY) {
        for (uint16_t client_id = 1; client_id < LQTT_MAX_CLIENTS; client_id++) {
            if (node->subs[topic_id][client_id] != 0 &&
                node->client_retain_index[topic_id][client_id] == slot_index) {
                node->client_retain_index[topic_id][client_id] = next_head;
            }
        }
    }

    node->retained[topic_id][slot_index].payload_length = payload_len;
    if (payload_len > 0) {
        memcpy(node->retained[topic_id][slot_index].payload, payload, payload_len);
    }
    node->retain_head[topic_id] = next_head;
}

// Finds the oldest valid slot index in the topic's circular retain ring,
// or returns LQTT_RETAIN_CAUGHT_UP if no retained messages exist.
static uint8_t find_oldest_retain_index(const lqtt_broker_node_t *node, uint8_t topic_id) {
    uint8_t head = node->retain_head[topic_id];
    // If head is occupied, the ring has wrapped and head points to the oldest message
    if (node->retained[topic_id][head].payload_length != LQTT_SLOT_EMPTY) {
        return head;
    }
    // Otherwise the ring has not wrapped yet, so slot 0 is the oldest if occupied
    if (node->retained[topic_id][0].payload_length != LQTT_SLOT_EMPTY) {
        return 0;
    }
    return LQTT_RETAIN_CAUGHT_UP;
}

// Sends the next queued retained message for the pair (topic_id, client_id) and advances the pair's read cursor
static void send_next_retained_for_client(broker_controller_t *controller, uint16_t client_id,
                                          uint8_t topic_id) {
    lqtt_broker_node_t *node = &controller->node;
    uint8_t sub_level = node->subs[topic_id][client_id];
    uint8_t slot_index = node->client_retain_index[topic_id][client_id];

    // Stop draining if the client unsubscribed or already caught up
    if (sub_level == 0 || slot_index == LQTT_RETAIN_CAUGHT_UP) {
        node->client_retain_index[topic_id][client_id] = LQTT_RETAIN_CAUGHT_UP;
        return;
    }

    const lqtt_retained_msg_t *slot = &node->retained[topic_id][slot_index];
    if (slot->payload_length == LQTT_SLOT_EMPTY) {
        node->client_retain_index[topic_id][client_id] = LQTT_RETAIN_CAUGHT_UP;
        return;
    }

    // Deliver using the client's subscribed QoS level (2 = QoS 1, 1 = QoS 0)
    bool qos1 = (sub_level == 2);
    lqtt_packet_t out_packet;
    if (!create_broker_publish_packet(node, client_id, topic_id, qos1, false,
                                      slot->payload, slot->payload_length, &out_packet)) {
        return;
    }

    if (!send_and_track(controller, client_id, topic_id, qos1, &out_packet)) {
        return;
    }

    // Advance cursor to the next slot, or mark caught up if we reached retain_head
    uint8_t next_slot_index = (slot_index + 1) % LQTT_MAX_RETAINED_MESSAGES;
    if (next_slot_index == node->retain_head[topic_id] ||
        node->retained[topic_id][next_slot_index].payload_length == LQTT_SLOT_EMPTY) {
        node->client_retain_index[topic_id][client_id] = LQTT_RETAIN_CAUGHT_UP;
    } else {
        node->client_retain_index[topic_id][client_id] = next_slot_index;
    }
}

// Registers a client's subscription, sends SUBACK, and starts sending retained messages
static void handle_subscribe(broker_controller_t *controller, uint16_t client_id,
                             const lqtt_packet_t *packet) {
    lqtt_broker_node_t *node = &controller->node;
    uint8_t topic_id = packet->topic_id;
    uint8_t qos_val = 1; // 1 = QoS 0
    if (packet->flags & LQTT_FLAG_QOS1) {
        qos_val = 2; // 2 = QoS 1
    }

    node->subs[topic_id][client_id] = qos_val;
    node->client_retain_index[topic_id][client_id] = find_oldest_retain_index(node, topic_id);

    lqtt_packet_t ack;
    if (create_suback_packet(node, client_id, topic_id, packet->msg_id, &ack)) {
        raw_send(controller->sock, BROKER_ID, (const uint8_t *)&ack, ack.length);
    }

    send_next_retained_for_client(controller, client_id, topic_id);
}

// Removes a client's subscription, clears pending retries on that topic, and sends UNSUBACK
static void handle_unsubscribe(broker_controller_t *controller, uint16_t client_id,
                               const lqtt_packet_t *packet) {
    lqtt_broker_node_t *node = &controller->node;
    uint8_t topic_id = packet->topic_id;

    node->subs[topic_id][client_id] = 0;
    node->client_retain_index[topic_id][client_id] = LQTT_RETAIN_CAUGHT_UP;

    // Cancel any active QoS 1 retries for this client/topic
    for (size_t i = 0; i < BROKER_MAX_INFLIGHT; i++) {
        if (controller->inflight[i].active &&
            controller->inflight[i].client_id == client_id &&
            controller->inflight[i].topic_id == topic_id) {
            controller->inflight[i].active = false;
        }
    }

    lqtt_packet_t ack;
    if (create_unsuback_packet(node, client_id, topic_id, packet->msg_id, &ack)) {
        raw_send(controller->sock, BROKER_ID, (const uint8_t *)&ack, ack.length);
    }
}

// Handles an incoming PUBLISH: ACKs QoS 1, prevents duplicate sends, stores retained data, and publishes to subscribers
static void handle_publish(broker_controller_t *controller, uint16_t client_id,
                           const lqtt_packet_t *packet) {
    lqtt_broker_node_t *node = &controller->node;
    uint8_t topic_id = packet->topic_id;
    bool is_qos1 = (packet->flags & LQTT_FLAG_QOS1) != 0;
    bool is_retain = (packet->flags & LQTT_FLAG_RETAIN) != 0;
    bool is_dupe = (packet->flags & LQTT_FLAG_DUPE) != 0;
    uint8_t payload_len = packet->length - LQTT_HEADER_LEN;

    // Always ACK a QoS 1 publish, even if it is a duplicate whose first ACK was lost
    // Note: QoS is "at least once"
    if (is_qos1) {
        lqtt_packet_t ack;
        if (create_broker_puback_packet(node, client_id, topic_id, packet->msg_id, &ack)) {
            raw_send(controller->sock, BROKER_ID, (const uint8_t *)&ack, ack.length);
        }

        // Drop duplicate retransmissions without re-storing or re-broadcasting
        if (is_dupe && controller->has_last_received[topic_id][client_id] &&
            controller->last_received_msg_id[topic_id][client_id] == packet->msg_id) {
            return;
        }
        controller->has_last_received[topic_id][client_id] = true;
        controller->last_received_msg_id[topic_id][client_id] = packet->msg_id;
    }

    if (is_retain) {
        store_retained(node, topic_id, packet->payload, payload_len);
    }

    // Publish to all subscribed clients
    for (uint16_t sub_id = 1; sub_id < LQTT_MAX_CLIENTS; sub_id++) {
        uint8_t sub_level = node->subs[topic_id][sub_id];
        if (sub_level == 0) {
            continue;
        }

        bool sub_qos1 = (sub_level == 2);
        lqtt_packet_t fwd;
        if (create_broker_publish_packet(node, sub_id, topic_id, sub_qos1, false,
                                         packet->payload, payload_len, &fwd)) {
            send_and_track(controller, sub_id, topic_id, sub_qos1, &fwd);
        }
    }
}

// Initializes the broker controller state, underlying node tables, socket, and PRNG seed
bool broker_controller_init(broker_controller_t *controller, int sock) {
    if (!controller || sock < 0) {
        return false;
    }

    memset(controller, 0, sizeof(*controller));
    if (!lqtt_broker_init(&controller->node)) {
        return false;
    }

    controller->sock = sock;

    // Seed PRNG for retransmission jitter
    struct timespec time;
    clock_gettime(CLOCK_MONOTONIC, &time);
    srand((unsigned int)(time.tv_nsec));

    return true;
}

// Validates an incoming Ethernet payload and dispatches it to the matching LQTT handler
void broker_process_packet(broker_controller_t *controller, const uint8_t *buf, size_t len) {
    if (!controller || !buf || len < LQTT_HEADER_LEN || len > sizeof(lqtt_packet_t)) {
        return;
    }

    // Account for 60-byte minimum Ethernet frame padding on the wire
    uint8_t wire_len = buf[0];
    if (wire_len < LQTT_HEADER_LEN || wire_len > len || wire_len > sizeof(lqtt_packet_t)) {
        return;
    }

    lqtt_packet_t packet;
    memset(&packet, 0, sizeof(packet));
    memcpy(&packet, buf, wire_len);

    // Drop corrupted packets
    if (compute_checksum(&packet) != packet.checksum) {
        return;
    }

    // Sanity check client ID and ignore broker sent messages
    uint16_t client_id = ntohs(packet.node_id);
    if (client_id == BROKER_ID || client_id >= LQTT_MAX_CLIENTS) {
        return;
    }

    // Handle packet based on type
    if (packet.msg_type == LQTT_MSG_SUBSCRIBE) {
        handle_subscribe(controller, client_id, &packet);
    } else if (packet.msg_type == LQTT_MSG_UNSUBSCRIBE) {
        handle_unsubscribe(controller, client_id, &packet);
    } else if (packet.msg_type == LQTT_MSG_PUBLISH) {
        handle_publish(controller, client_id, &packet);
    } else if (packet.msg_type == LQTT_MSG_PUBLISH_ACK) {
        clear_inflight(controller, client_id, packet.topic_id, packet.msg_id);
    }
}

// Advances retransmission timers by 1 tick and sends pending retained messages to clients who've missed them
void broker_controller_tick(broker_controller_t *controller) {
    if (!controller) {
        return;
    }

    // Advance QoS 1 retransmission countdowns by 1 tick
    for (size_t i = 0; i < BROKER_MAX_INFLIGHT; i++) {
        broker_inflight_t *slot = &controller->inflight[i];
        if (!slot->active) {
            continue;
        }

        if (slot->ticks_remaining > 0) {
            slot->ticks_remaining--;
        }

        // If a timer expires: give up if max retries was reached, otherwise mark as a dupe packet
        // and retransmit
        if (slot->ticks_remaining == 0) {
            if (slot->retries >= BROKER_MAX_RETRIES) {
                slot->active = false;
                continue;
            }

            slot->retries++;
            slot->packet.flags |= LQTT_FLAG_DUPE;
            slot->packet.checksum = compute_checksum(&slot->packet);
            slot->ticks_remaining = BROKER_BASE_RETRY_TICKS + next_jitter_ticks();

            raw_send(controller->sock, BROKER_ID, (const uint8_t *)&slot->packet,
                     slot->packet.length);
        }
    }

    // Send retained messages to newly subbed clients
    // Why do we put this here instead of handling when the client sent us the sub packet?
    // Avoid flooding the client and we send 1 retained message during sub already
    for (uint16_t topic = 0; topic < LQTT_MAX_TOPICS; topic++) {
        for (uint16_t client_id = 1; client_id < LQTT_MAX_CLIENTS; client_id++) {
            if (controller->node.client_retain_index[topic][client_id] != LQTT_RETAIN_CAUGHT_UP) {
                send_next_retained_for_client(controller, client_id, (uint8_t)topic);
            }
        }
    }
}