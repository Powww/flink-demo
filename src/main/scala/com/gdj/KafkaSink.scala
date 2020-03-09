package com.gdj

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * @author gdj
 * @create 2020-03-02-14:24
 *
 */
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataDS: DataStream[String] = env.readTextFile("input/data.txt")

    // 向Kafka中输出内容
    dataDS.addSink(new FlinkKafkaProducer011[String]("hadoop01:9092", "sensor", new SimpleStringSchema()))

  }
}
