#pragma once

#include <stddef.h>
#include <stdint.h>

// We use define CUSTOM_ETHERTYPE to be used in the EtherType field to 
// distinguish our packets from other packets on the ethernet wire
#define CUSTOM_ETHERTYPE 0x88B5
// Ensure our protocol packet length is adhered to
#define MAX_PACKET_LENGTH  128

// Used to start connect to the Ethernet frames for our protocol
int raw_open(const char *ifname);
// Used to send a raw packet of our protocol bundled in a broadcast ethernet frame
// to emulate a radio link
int raw_send(int sock, uint16_t node_id, const uint8_t *payload, size_t len);
// Used to receive a raw packet bundled in an ethernet frame and outputs the 
// stripped packet into out_buf
int raw_recv(int sock, uint8_t *out_buf, size_t max_len);