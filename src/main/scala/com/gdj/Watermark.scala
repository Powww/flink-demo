package com.gdj


import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author gdj
 * @create 2020-03-03-10:05
 *
 */
object Watermark {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataDS = env.socketTextStream("127.0.0.1", 9999)
    val sensorDS = dataDS.map(
      data => {
        val datas = data.split(",")
        WaterSensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )
    val markDS = sensorDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
      override def extractTimestamp(element: WaterSensor): Long = {
        element.ts * 1000L
      }
    })
    val applyDS = markDS.keyBy(_.id).timeWindow(Time.seconds(5)).apply(
      (key: String, window: TimeWindow, datas: Iterable[WaterSensor], out: Collector[String]) => {
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val start = window.getStart
        val end = window.getEnd
        out.collect(s"[$start-$end],数据[$datas]")
      }
    )

    markDS.print("mark>>>")
    applyDS.print("window>>>")
    env.execute()

  }

}
