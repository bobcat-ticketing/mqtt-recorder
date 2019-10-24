# MQTT Recorder

Simple tool to record/replay MQTT data.

## Usage

```
usage: mqtt_recorder.py [-h] [--server server] [--mode mode]
                        [--input filename] [--output filename] [--realtime]
                        [--speed factor] [--delay milliseconds] [--debug]

MQTT recorder

optional arguments:
  -h, --help            show this help message and exit
  --server server       MQTT broker
  --mode mode           Mode of operation (record/replay)
  --input filename      Input file
  --output filename     Output file
  --realtime            Enable realtime replay
  --speed factor        Realtime speed factor for replay (10=10x)
  --delay milliseconds  Delay between replayed events
  --debug               Enable debugging
```
