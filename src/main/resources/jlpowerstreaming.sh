#!/usr/bin/env bash


# java -cp /home/spark/cgy/sparkstreaming/jl-power-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar /home/spark/cgy/sparkstreaming/jlpowertestdata jlpowerTopic 1
# please: set ff=unix
# vms01.vms.com:9092,vms02.vms.com:9092,vms03.vms.com:9092
brokers="vms01.vms.com:6667"
topics="jlpowerTopic"
batchinterval="5 second"
# vms01.vms.com:2181,vms03.vms.com:2181,vms03.vms.com:2181
zkHosts="vms01.vms.com:2181,vms02.vms.com:2181,vms03.vms.com:2181"

/usr/hdp/2.6.0.3-8/spark2/bin/spark-submit --class com.jl.streaming.JLTransformerLoadRate \
--master yarn \
--deploy-mode cluster \
--driver-memory 4G \
--driver-cores 2 \
--driver-java-options "-Dweb.streaming.shutdown.filepath=/user/cgy/sparkstreaming/stop -Xms=4G" \
--conf "spark.executor.extraJavaOptions=-Dweb.streaming.shutdown.filepath=/user/cgy/sparkstreaming/stop -Xms6G" \
--executor-memory 6G \
--num-executors 3 \
--executor-cores 2 \
--conf spark.streaming.backpressure.enabled=true \
/home/spark/cgy/sparkstreaming/jl-power-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar ${brokers} ${topics} "${batchinterval}" ${zkHosts}