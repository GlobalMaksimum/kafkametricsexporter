exports kafka metrics from a kafka topic to influx written by https://github.com/GlobalMaksimum/kafkametricreporter

also exports kafka consumer group metrics to influx with "group_offsets" measurement name

current version just supports kerberos enabled clusters.

application is built with sbt-native packager and kafkainfluxexporter is using jaas file under conf 