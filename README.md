# MQTT Recorder

Simple tool to record/replay MQTT data.

```
usage: mqtt_recorder.py [-h] [--server server] [--mode mode]
                        [--delay milliseconds] [--input filename]
                        [--output filename] [--realtime] [--debug]

MQTT recorder

optional arguments:
  -h, --help            show this help message and exit
  --server server       MQTT broker
  --mode mode           Mode of operation (record/replay)
  --delay milliseconds  Delay between replayed events
  --input filename      Input file
  --output filename     Output file
  --realtime            Enable realtime replay
  --debug               Enable debugging
```
