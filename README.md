InfluxDB sink
================

This influxdb sink is compatible with newer versions of InfluxDB ( > 0.9 ).
In its current form it reads influxdb compliant data from the body of the flume events.

To deploy it, copy the jars of the following components in the flume classpath:

flume-ng-influxdb-sink

influxdb-java-2.1

okhttp-2.4.0

okio-1.8.0

retrofit-1.9.0

Here is an example sink configuration:

```
agent.sinks.influx.type = com.flumetest.influxdb.InfluxSink
agent.sinks.influx.host = influx-host
agent.sinks.influx.port = 8086
agent.sinks.influx.username = kukta
agent.sinks.influx.password = fafaf
agent.sinks.influx.database = flumetest
agent.sinks.influx.channel = memoryChannel
```
