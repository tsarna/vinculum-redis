// Package stream implements Redis/Valkey Streams integration for Vinculum:
// a producer that maps bus events to XADD, and a consumer that reads entries
// via XREADGROUP and delivers them to the vinculum bus.
package stream
