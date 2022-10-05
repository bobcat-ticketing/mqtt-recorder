"""MQTT recorder"""

import argparse
import asyncio
import base64
import json
import logging
import os
import signal
import sys
import time

from asyncio_mqtt import Client

logger = logging.getLogger("mqtt_recorder")


async def mqtt_record(client: Client, output: str = None, topic: str = "#") -> None:
    """Record MQTT messages"""
    if output is not None:
        output_file = open(output, "wt")
    else:
        output_file = sys.stdout
    await client.connect()
    await client.subscribe(topic)
    async with client.unfiltered_messages() as messages:
        async for message in messages:
            record = {
                "time": time.time(),
                "qos": message.qos,
                "retain": message.retain,
                "topic": message.topic,
                "msg_b64": base64.urlsafe_b64encode(message.payload).decode(),
            }
            logger.info("Received: %s", record)
            print(json.dumps(record), file=output_file)
            output_file.flush()


async def mqtt_replay(
    client: Client,
    input: str = None,
    delay: int = 0,
    realtime: bool = False,
    scale: float = 1,
) -> None:
    """Replay MQTT messages"""
    await client.connect()
    if input is not None:
        input_file = open(input, "rt")
    else:
        input_file = sys.stdin
    if delay > 0:
        static_delay_s = delay / 1000
    else:
        static_delay_s = 0
    last_timestamp = None
    for line in input_file:
        record = json.loads(line)
        logger.info("%s", record)
        if "msg_b64" in record:
            msg = base64.urlsafe_b64decode(record["msg_b64"].encode())
        elif "msg" in record:
            msg = record["msg"].encode()
        else:
            logger.warning("Missing message attribute: %s", record)
            next
        logger.info("Publish: %s", record)
        await client.publish(
            record["topic"],
            msg,
            retain=record.get("retain"),
            qos=record.get("qos", 0),
        )
        delay_s = static_delay_s
        if realtime or scale != 1:
            delay_s += (
                record["time"] - last_timestamp if last_timestamp else 0
            ) * scale
            last_timestamp = record["time"]
        if delay_s > 0:
            logger.debug("Sleeping %.3f seconds", delay_s)
            await asyncio.sleep(delay_s)


async def shutdown(sig, loop):
    loop.stop()
    os._exit(-1)


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="MQTT recorder")

    parser.add_argument(
        "--server",
        dest="server",
        metavar="server",
        help="MQTT broker",
        default="127.0.0.1",
    )
    parser.add_argument(
        "--mode",
        dest="mode",
        metavar="mode",
        choices=["record", "replay"],
        help="Mode of operation (record/replay)",
        default="record",
    )
    parser.add_argument("--input", dest="input", metavar="filename", help="Input file")
    parser.add_argument(
        "--output", dest="output", metavar="filename", help="Output file"
    )
    parser.add_argument(
        "--realtime",
        dest="realtime",
        action="store_true",
        help="Enable realtime replay",
    )
    parser.add_argument(
        "--speed",
        dest="speed",
        type=float,
        default=1,
        metavar="factor",
        help="Realtime speed factor for replay (10=10x)",
    )
    parser.add_argument(
        "--delay",
        dest="delay",
        type=int,
        default=0,
        metavar="milliseconds",
        help="Delay between replayed events",
    )
    parser.add_argument(
        "--debug", dest="debug", action="store_true", help="Enable debugging"
    )

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    client = Client(hostname=args.server)

    if args.mode == "replay":
        process = mqtt_replay(
            client=client,
            input=args.input,
            delay=args.delay,
            realtime=args.realtime,
            scale=1 / args.speed,
        )
    else:
        process = mqtt_record(client=client, output=args.output)

    loop = asyncio.get_event_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, lambda: asyncio.ensure_future(shutdown(s, loop)))

    loop.run_until_complete(process)


if __name__ == "__main__":
    main()
