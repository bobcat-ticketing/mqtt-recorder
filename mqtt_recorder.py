"""MQTT recorder"""

import argparse
import asyncio
import json
import logging
import sys
import base64
import time
from hbmqtt.client import MQTTClient, QOS_0, QOS_1


TOPICS = [("#", QOS_1)]

logger = logging.getLogger('mqtt_recorder')


async def mqtt_record(server: str, output: str = None):
    """Record MQTT messages"""
    mqtt = MQTTClient()
    await mqtt.connect(server)
    await mqtt.subscribe(TOPICS)
    if output is not None:
        output_file = open(output, 'wt')
    else:
        output_file = sys.stdout
    while True:
        message = await mqtt.deliver_message()
        record = {
            'time': time.time(),
            'qos': message.qos,
            'retain': message.retain,
            'topic': message.topic,
            'msg_b64': base64.urlsafe_b64encode(message.data).decode()
        }
        print(json.dumps(record), file=output_file)


async def mqtt_replay(server: str, input: str = None):
    """Replay MQTT messages"""
    mqtt = MQTTClient()
    await mqtt.connect(server)
    await mqtt.subscribe(TOPICS)
    if input is not None:
        input_file = open(input, 'rt')
    else:
        input_file = sys.stdin
    for line in input_file:
        record = json.loads(line)
        logger.info("%s", record)
        if 'msg_b64' in record:
            msg = base64.urlsafe_b64decode(record['msg_b64'].encode())
        elif 'msg' in record:
            msg = record['msg'].encode()
        else:
            logger.warning("Missing message attribute: %s", record)
            next
        logger.info("Publish: %s", record)
         await mqtt.publish(record['topic'], msg,
                                 retain=record.get('retain'),
                                 qos=record.get('qos', QOS_0))


def main():
    """ Main function"""
    parser = argparse.ArgumentParser(description='MQTT recorder')

    parser.add_argument('--server',
                        dest='server',
                        metavar='server',
                        help='MQTT broker',
                        default='mqtt://127.0.0.1/')
    parser.add_argument('--mode',
                        dest='mode',
                        metavar='mode',
                        choices=['record', 'replay'],
                        help='Mode of operation (record/replay)',
                        default='record')
    parser.add_argument('--input',
                        dest='input',
                        metavar='filename',
                        help='Input file')
    parser.add_argument('--output',
                        dest='output',
                        metavar='filename',
                        help='Output file')
    parser.add_argument('--debug',
                        dest='debug',
                        action='store_true',
                        help="Enable debugging")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if args.mode == 'replay':
        process = mqtt_replay(server=args.server, input=args.input)
    else:
        process = mqtt_record(server=args.server, output=args.output)

    asyncio.get_event_loop().run_until_complete(process)


if __name__ == "__main__":
    main()
