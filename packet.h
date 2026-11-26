#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// Values are defined by the protocol spec or trivial math
#define LQTT_MAX_PACKET_LEN 128
#define LQTT_HEADER_LEN 8
#define LQTT_MAX_PAYLOAD_LEN 120 // max_packet_length - header_len = 128 - 8 = 120

// Flags
#define LQTT_FLAG_QOS1   (1 << 0) // 0x01: 1 = QoS 1, 0 = QoS 0
#define LQTT_FLAG_RETAIN (1 << 1) // 0x02: 1 = Retain message on broker
#define LQTT_FLAG_DUPE   (1 << 2) // 0x04: 1 = Retransmitted duplicate packet

// Message Types
typedef enum {
    LQTT_MSG_SUBSCRIBE       = 0x01,
    LQTT_MSG_SUBSCRIBE_ACK   = 0x02,
    LQTT_MSG_UNSUBSCRIBE     = 0x03,
    LQTT_MSG_UNSUBSCRIBE_ACK = 0x04,
    LQTT_MSG_PUBLISH         = 0x05,
    LQTT_MSG_PUBLISH_ACK     = 0x06
} lqtt_msg_type_t;

// 8-byte header + payload (up to 120 bytes) with max packet of 128 bytes
typedef struct __attribute__((packed)) {
    uint8_t  length;   // Byte 0: Total packet length in bytes: between 8 and 128
    uint8_t  msg_type; // Byte 1: What type of message this is
    uint8_t  flags;    // Byte 2: Any flags: QoS, Retain, Dupe
    uint8_t  topic_id; // Byte 3: Topic ID: between 0 and 255
    uint16_t node_id;  // Bytes 4-5: Sender/Target Node ID in network byte order
    uint8_t  msg_id;   // Byte 6: Message ID (broker-client specific)
    uint8_t  checksum; // Byte 7: 1 byte checksum of the packet except the checksum itself
    uint8_t  payload[LQTT_MAX_PAYLOAD_LEN]; // Bytes 8 to 127: Data bytes
} lqtt_packet_t;

// Computes the checksum over a packet using
uint8_t compute_checksum(const lqtt_packet_t *packet);

// Verify that a packet's checksum is valid
bool verify_packet_checksum(const lqtt_packet_t *packet, , size_t raw_len);