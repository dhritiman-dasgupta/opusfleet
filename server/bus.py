"""Kafka producer/consumer construction, in one place.

Topic design:

    audio.raw      key=imei  value=RTP header + Opus packet (exactly the bytes
                             off the wire). Keying by IMEI puts one device on one
                             partition, which is what preserves frame order --
                             Kafka guarantees order per partition, not per topic.
    device.stat    key=imei  value=the modem's STAT json
    device.events  key=imei  value=connect/disconnect
    device.levels  key=imei  value=per-second dBFS, produced by the levels role

Keeping the RTP header in the payload means sequence numbers survive into every
consumer, so loss can be measured downstream instead of only at the socket.
"""

import json
import socket

from confluent_kafka import Consumer, Producer

from . import config

ALL_TOPICS = ("audio.raw", "device.stat", "device.events", "device.levels")


def make_producer(client_id="opusfleet"):
    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "client.id": f"{client_id}-{socket.gethostname()}",
        "compression.type": config.KAFKA_COMPRESSION,
        "linger.ms": config.KAFKA_LINGER_MS,
        "acks": config.KAFKA_ACKS,
        # 500 devices x 50 frames/s = 25k msg/s; the default 100k-message queue
        # is ~4 s of headroom, enough to ride out a broker hiccup without
        # dropping frames on the floor.
        "queue.buffering.max.messages": 1_000_000,
        "queue.buffering.max.kbytes": 1_048_576,
        "message.max.bytes": 1_000_000,
    })


def make_consumer(group_id, topics, offset="latest", client_id="opusfleet"):
    c = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "client.id": f"{client_id}-{socket.gethostname()}",
        "auto.offset.reset": offset,
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        "fetch.min.bytes": 1,
        "session.timeout.ms": 45000,
        "max.partition.fetch.bytes": 4 * 1024 * 1024,
        # A topic created after this consumer subscribes is invisible until the
        # next metadata refresh; the 5-minute default is far too long to notice.
        "topic.metadata.refresh.interval.ms": 30000,
        "allow.auto.create.topics": False,
    })
    c.subscribe(list(topics))
    return c


def jdump(obj):
    return json.dumps(obj, separators=(",", ":")).encode()


def jload(raw):
    return json.loads(raw.decode())
