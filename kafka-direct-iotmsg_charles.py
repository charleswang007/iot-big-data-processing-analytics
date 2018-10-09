
"""
 Processes direct stream from kafka, '\n' delimited text directly received
   every 5 seconds.
 Usage: kafka-direct-iotmsg.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a
   producer first, see:
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iotmsg.py \
      localhost:9092 iotmsgs`
"""
from __future__ import print_function

import sys
import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from operator import add


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-direct-iotmsg.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 5)

    sc.setLogLevel("WARN")

    ###############
    # Globals
    ###############

    total_gforce = 0.0
    count_gforce = 0
    avg_gforce = 0.0
    max_gforce = 0.0
    total_avelocity = 0.0
    count_avelocity = 0
    avg_avelocity = 0.0
    max_avelocity = 0.0
    total_bpressure = 0.0
    count_bpressure = 0
    avg_bpressure = 0.0
    max_bpressure = 0.0

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # Read in the Kafka Direct Stream into a TransformedDStream
    lines = kvs.map(lambda x: x[1])
    jsonLines = lines.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))

    ###############
    # Processing
    ###############

    # foreach function to iterate over each RDD of a DStream
    def processGForceRDD(time, rdd):
      # Match local function variables to global variables
      global total_gforce
      global count_gforce
      global avg_gforce
      global max_gforce

      dataList = rdd.collect()
      for dataFloat in dataList:
        total_gforce += float(dataFloat)
        count_gforce += 1
        avg_gforce = total_gforce / count_gforce
        if float(dataFloat) > max_gforce:
            max_gforce = float(dataFloat)

      print("===============================")
      print("Data Type: G-FORCE")
      print("Data List:", dataList)
      print("Total = " + str(total_gforce))
      print("Count = " + str(count_gforce))
      print("Avg = " + str(avg_gforce))
      print("Max = " + str(max_gforce))

    def processAngularVelocityRDD(time, rdd):
      # Match local function variables to global variables
      global total_avelocity
      global count_avelocity
      global avg_avelocity
      global max_avelocity

      dataList = rdd.collect()
      for dataFloat in dataList:
        total_avelocity += float(dataFloat)
        count_avelocity += 1
        avg_avelocity = total_avelocity / count_avelocity
        if float(dataFloat) > max_avelocity:
            max_avelocity = float(dataFloat)

      print("===============================")
      print("Data Type: ANGULAR-VELOCITY")
      print("Data List:", dataList)
      print("Total = " + str(total_avelocity))
      print("Count = " + str(count_avelocity))
      print("Avg = " + str(avg_avelocity))
      print("Max = " + str(max_avelocity))

    def processBarometricPressureRDD(time, rdd):
      # Match local function variables to global variables
      global total_bpressure
      global count_bpressure
      global avg_bpressure
      global max_bpressure

      dataList = rdd.collect()
      for dataFloat in dataList:
        total_bpressure += float(dataFloat)
        count_bpressure += 1
        avg_bpressure = total_bpressure / count_bpressure
        if float(dataFloat) > max_bpressure:
            max_bpressure = float(dataFloat)

      print("===============================")
      print("Data Type: BAROMETRIC PRESSURE")
      print("Data List:", dataList)
      print("Total = " + str(total_bpressure))
      print("Count = " + str(count_bpressure))
      print("Avg = " + str(avg_bpressure))
      print("Max = " + str(max_bpressure))

    # Search for specific IoT data values (assumes jsonLines are split(','))
    gforceValues = jsonLines.filter(lambda x: re.findall(r"g-force.*", x, 0))
    gforceValues.pprint(num=10000)

    # Parse out just the value without the JSON key
    parsedGforceValues = gforceValues.map(lambda x: re.sub(r"\"g-force\":", "", x).split(',')[0])

    # Search for specific IoT data values (assumes jsonLines are split(','))
    angularVelocityValues = jsonLines.filter(lambda x: re.findall(r"angular-velocity.*", x, 0))
    angularVelocityValues.pprint(num=10000)

    # Parse out just the value without the JSON key
    parsedAngularVelocityValues = angularVelocityValues.map(lambda x: re.sub(r"\"angular-velocity\":", "", x).split(',')[0])
  
    # Search for specific IoT data values (assumes jsonLines are split(','))
    bpressureValues = jsonLines.filter(lambda x: re.findall(r"barometric-pressure.*", x, 0))
    bpressureValues.pprint(num=10000)

    # Parse out just the value without the JSON key
    parsedBarometricPressureValues = bpressureValues.map(lambda x: re.sub(r"\"barometric-pressure\":", "", x).split(',')[0])

    # Count how many values were parsed
    countMap = parsedGforceValues.map(lambda x: 1).reduce(add)
    valueCount = countMap.map(lambda x: "Total Count of Msgs: " + unicode(x))
    valueCount.pprint()

    # Sort g-force data
    sortedValues = parsedGforceValues.transform(lambda x: x.sortBy(lambda y: y))
    sortedValues.pprint(num=10000)
    
    # Sort angular-velocity data
    sortedValues = parsedAngularVelocityValues.transform(lambda x: x.sortBy(lambda y: y))
    sortedValues.pprint(num=10000)

    # Sort barometric-pressure data
    sortedValues = parsedBarometricPressureValues.transform(lambda x: x.sortBy(lambda y: y))
    sortedValues.pprint(num=10000)

    # Iterate on each RDD in parsedGforceValues DStream to use w/global variables
    # Processing: G-Force
    parsedGforceValues.foreachRDD(processGForceRDD)

    # Processing: Angular-Velocity
    parsedAngularVelocityValues.foreachRDD(processAngularVelocityRDD)

    # Processing: Barometric-Pressure
    parsedBarometricPressureValues.foreachRDD(processBarometricPressureRDD)

    ssc.start()
    ssc.awaitTermination()
