#include "client_controller.h"
#include "lqtt_client.h"
#include "raw_ethernet.h"

#include <arpa/inet.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define LQTT_BROKER_ID 0

// Generates jitter to reduce collisions on retransmissions
static uint16_t next_jitter_ticks(void) {
    return (uint16_t)(rand() % (CLIENT_MAX_JITTER_TICKS + 1));
}

// Sends a packet and if needed enters it into the retransmission tracking system
static bool send_and_track(client_controller_t *controller, bool track_ack,
                           uint8_t expected_ack, const lqtt_packet_t *packet) {
    if (raw_send(controller->sock, controller->client_id, (const uint8_t *)packet, packet->length) < 0) {
        return false;
    }

    if (!track_ack) {
        return true;
    }

    for (size_t i = 0; i < CLIENT_MAX_INFLIGHT; i++) {
        if (!controller->inflight[i].active) {
            controller->inflight[i].active = true;
            controller->inflight[i].expected_ack = expected_ack;
            controller->inflight[i].topic_id = packet->topic_id;
            controller->inflight[i].msg_id = packet->msg_id;
            controller->inflight[i].retries = 0;
            controller->inflight[i].ticks_remaining = CLIENT_BASE_RETRY_TICKS + next_jitter_ticks();
            memcpy(&controller->inflight[i].packet, packet, sizeof(*packet));
            return true;
        }
    }

    return false;
}

// Clears an active inflight entry - used when its matching ACK arrives from the broker
static void clear_inflight(client_controller_t *controller, uint8_t ack_type,
                           uint8_t topic_id, uint8_t msg_id) {
    for (size_t i = 0; i < CLIENT_MAX_INFLIGHT; i++) {
        client_inflight_t *slot = &controller->inflight[i];
        if (slot->active &&
            slot->expected_ack == ack_type &&
            slot->topic_id == topic_id &&
            slot->msg_id == msg_id) {
            if (ack_type == LQTT_MSG_SUBSCRIBE_ACK) {
                uint8_t qos_val = (slot->packet.flags & LQTT_FLAG_QOS1) ? 2 : 1;
                controller->subbed_topics[topic_id] = qos_val;
                controller->node.subbed_topics[topic_id] = qos_val;
            } else if (ack_type == LQTT_MSG_UNSUBSCRIBE_ACK) {
                controller->subbed_topics[topic_id] = 0;
                controller->node.subbed_topics[topic_id] = 0;
            }
            slot->active = false;
            return;
        }
    }
}

// Handles an incoming PUBLISH from the broker. ACKs QoS 1, suppresses duplicates, and triggers on_message
// callback
static void handle_broker_publish(client_controller_t *controller, const lqtt_packet_t *packet) {
    uint8_t topic_id = packet->topic_id;
    bool is_qos1 = (packet->flags & LQTT_FLAG_QOS1) != 0;
    bool is_dupe = (packet->flags & LQTT_FLAG_DUPE) != 0;
    uint8_t payload_len = packet->length - LQTT_HEADER_LEN;

    // Always ACK a QoS 1 publish from the broker, even if it is a duplicate whose first ACK was lost
    if (is_qos1) {
        lqtt_packet_t ack;
        if (create_puback_packet(&controller->node, topic_id, packet->msg_id, &ack)) {
            raw_send(controller->sock, controller->client_id, (const uint8_t *)&ack, ack.length);
        }

        // Drop duplicate retransmissions without re-delivering to the application
        if (is_dupe && controller->has_last_received[topic_id] &&
            controller->last_received_msg_id[topic_id] == packet->msg_id) {
            return;
        }
        controller->has_last_received[topic_id] = true;
        controller->last_received_msg_id[topic_id] = packet->msg_id;
    }

    // Ignore publishes for topics we are no longer subscribed to
    if (controller->node.subbed_topics[topic_id] == 0) {
        return;
    }

    if (controller->on_message != NULL) {
        controller->on_message(topic_id, packet->payload, payload_len);
    }
}

bool client_controller_init(client_controller_t *controller, int sock, uint16_t client_id,
                            client_message_callback_t on_message) {
    if (!controller || sock < 0 || client_id == LQTT_BROKER_ID) {
        return false;
    }

    memset(controller, 0, sizeof(*controller));
    if (!lqtt_client_init(client_id, &controller->node)) {
        return false;
    }

    controller->client_id = client_id;
    controller->sock = sock;
    controller->on_message = on_message;

    // Seed PRNG for retransmission jitter
    struct timespec time;
    clock_gettime(CLOCK_MONOTONIC, &time);
    srand((unsigned int)(time.tv_nsec));

    return true;
}

bool client_subscribe(client_controller_t *controller, uint8_t topic_id, bool qos1) {
    if (!controller) {
        return false;
    }

    lqtt_packet_t packet;
    uint8_t qos = qos1 ? 1 : 0;
    if (!create_subscribe_packet(&controller->node, topic_id, qos, &packet)) {
        return false;
    }

    return send_and_track(controller, true, LQTT_MSG_SUBSCRIBE_ACK, &packet);
}

bool client_unsubscribe(client_controller_t *controller, uint8_t topic_id) {
    if (!controller) {
        return false;
    }

    controller->has_last_received[topic_id] = false;

    // Cancel any active QoS 1 retries for this topic
    for (size_t i = 0; i < CLIENT_MAX_INFLIGHT; i++) {
        if (controller->inflight[i].active &&
            controller->inflight[i].topic_id == topic_id) {
            controller->inflight[i].active = false;
        }
    }

    lqtt_packet_t packet;
    if (!create_unsubscribe_packet(&controller->node, topic_id, &packet)) {
        return false;
    }

    return send_and_track(controller, true, LQTT_MSG_UNSUBSCRIBE_ACK, &packet);
}

bool client_publish(client_controller_t *controller, uint8_t topic_id, bool qos1, bool retain,
                    const uint8_t *payload, uint8_t payload_len) {
    if (!controller || payload_len > LQTT_MAX_PAYLOAD_LEN) {
        return false;
    }
    if (payload_len > 0 && !payload) {
        return false;
    }

    lqtt_packet_t packet;
    if (!create_publish_packet(&controller->node, topic_id, qos1, retain, false,
                               payload, payload_len, &packet)) {
        return false;
    }

    return send_and_track(controller, qos1, LQTT_MSG_PUBLISH_ACK, &packet);
}

void client_process_packet(client_controller_t *controller, const uint8_t *buf, size_t len) {
    if (!controller || !buf || len < LQTT_HEADER_LEN || len > sizeof(lqtt_packet_t)) {
        return;
    }

    // Account for 60-byte minimum Ethernet frame padding on the wire
    uint8_t wire_len = buf[0];
    if (wire_len < LQTT_HEADER_LEN || wire_len > len) {
        return;
    }

    lqtt_packet_t packet;
    memset(&packet, 0, sizeof(packet));
    memcpy(&packet, buf, wire_len);

    // Drop corrupted packets
    if (compute_checksum(&packet) != packet.checksum) {
        return;
    }

    // Ignore packets not addressed to this client
    uint16_t target_id = ntohs(packet.node_id);
    if (target_id != controller->client_id) {
        return;
    }

    if (packet.msg_type == LQTT_MSG_SUBSCRIBE_ACK) {
        clear_inflight(controller, LQTT_MSG_SUBSCRIBE_ACK, packet.topic_id, packet.msg_id);
    } else if (packet.msg_type == LQTT_MSG_UNSUBSCRIBE_ACK) {
        clear_inflight(controller, LQTT_MSG_UNSUBSCRIBE_ACK, packet.topic_id, packet.msg_id);
    } else if (packet.msg_type == LQTT_MSG_PUBLISH_ACK) {
        clear_inflight(controller, LQTT_MSG_PUBLISH_ACK, packet.topic_id, packet.msg_id);
    } else if (packet.msg_type == LQTT_MSG_PUBLISH) {
        handle_broker_publish(controller, &packet);
    }
}

void client_controller_tick(client_controller_t *controller) {
    if (!controller) {
        return;
    }

    for (size_t i = 0; i < CLIENT_MAX_INFLIGHT; i++) {
        // Loop through the inflight messages and decrement the ticks_remaining
        client_inflight_t *slot = &controller->inflight[i];
        if (!slot->active) {
            continue;
        }

        if (slot->ticks_remaining > 0) {
            slot->ticks_remaining--;
        }

        // If ticks hits 0, retransmit
        if (slot->ticks_remaining == 0) {
            // Give up on packet if at max retries
            if (slot->retries >= CLIENT_MAX_RETRIES) {
                slot->active = false;
                continue;
            }

            slot->retries++;
            slot->packet.flags |= LQTT_FLAG_DUPE;
            slot->packet.checksum = compute_checksum(&slot->packet);
            slot->ticks_remaining = CLIENT_BASE_RETRY_TICKS + next_jitter_ticks();

            raw_send(controller->sock, controller->client_id, (const uint8_t *)&slot->packet,
                     slot->packet.length);
        }
    }
}