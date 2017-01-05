flume-hdfs-avro-serializer
===============

Unique class which allows to write avro byte array to HDFS with flume component.
In flume configuration file, use the following properties :

agent.sinks.hdfsAvroSink.serializer = fr.ippon.flume.AvroEventSerializer$Builder
agent.sinks.hdfsAvroSink.serializer.schema.path = /path/to/avro/schema.avsc


N.B : mapr depdendencies used can be replaced
