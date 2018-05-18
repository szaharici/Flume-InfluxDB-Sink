InfluxDB sink
================

This influxdb sink is compatible with newer versions of InfluxDB ( > 0.9 ).
It can read selected data from flume events, or it can be used to relay compilant data from flume event body to InfluxDB.
The current version was tested with InfluxDB 1.3.4

Build
=======

The code could be built with maven.
```
mvn package
```

Installation
==========

To deploy it, copy flume-influxdb-sink-0.0.2.jar and its dependencies in the flume classpath. A fat jar including all the dependencies in build by maven as well so it can be copied as well.


Configuration
=========

Here is an example sink configuration:

```
agent.sinks.influx.type = com.flumetest.influxdb.InfluxSink
agent.sinks.influx.host = influx-host
agent.sinks.influx.port = 8086
agent.sinks.influx.username = kukta
agent.sinks.influx.password = fafaf
agent.sinks.influx.database = flumetest
agent.sinks.influx.metricnamefromfield = true
agent.sinks.influx.metricnamefield = kukta
agent.sinks.influx.metrcivaluefromfield = value
agent.sinks.influx.tagsfromfields = kukta,blah
agent.sinks.influx.channel = memoryChannel
```
The sink supports the following configuration parameters:
```
 host = The DNS or IP of the InfluxDB host, default localhost 
 port = The port on which influxdb listens, default 8086 
 database = The database where to write data, default "flumetest"    
 batchSize = The number of metrics sent simultaneously to InfluxDB, default 100 
 username = InfluxDB username, default root 
 password = InfluxDB password, default root 
 influxdatafrombody = Whether to read ready-made influxdata from the event body, default false 
 metricnamefromfield = Whether to read the metric name from a flume header, default false 
 metricnamefield = The name of the header from which to read the metric name, default "flume_metric". 
 The option is ignored if metricnamefromfield is set to false  
 metricnamefromconf = Whether to set a metric name for all the events, default "flume_metric". 
 The option is ignored if metricnamefromfield is set to true
 metrcivaluefromfield = The flume header from which to read the metric value 
 tagsfromfields = Comma separated list of flume headers that will be added as tags 
 timestampfromfield = Header name from where to read the timestamp, default "timestamp"). 
 If no fields are available a good option is to add the flume timestamp interceptor to 
 the flume source https://flume.apache.org/FlumeUserGuide.html#timestamp-interceptor 
```
