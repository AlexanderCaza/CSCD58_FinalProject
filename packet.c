#include "packet.h"

uint8_t compute_checksum(const lqtt_packet_t *packet) {
    const uint8_t *bytes = (const uint8_t *)packet;
    uint16_t sum = 0;

    for (int i = 0; i < packet->length; i++) {
        // Skip the checksum field
        if (i == 7) continue;
        sum += bytes[i];
    }

    // Shift the carry into the first bit
    while (sum >> 8) {
        sum = (sum & 0xFF) + (sum >> 8);
    }

    return (uint8_t)~sum; // One's complement so an all-zero packet doesn't checksum to 0x00
}

bool verify_packet_checksum(const lqtt_packet_t *packet, size_t raw_len) {
    // If the packet size breaks the spec
    if (packet->length < 8 || packet->length > 128) return false;
    // If the packet is claiming to be bigger than it is
    if (raw_len < packet->length) return false;
    // Return whether the checksum matches what we compute
    return compute_checksum(packet) == packet->checksum;
}
