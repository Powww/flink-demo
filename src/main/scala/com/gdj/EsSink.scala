package com.gdj

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests


/**
 * @author gdj
 * @create 2020-03-02-14:24
 *
 */
object EsSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val dataDS = env.fromCollection(
      List("a", "b", "c")
    )

    val httpHosts =new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop104",9200))

    val ds = dataDS.map(
      s => {
        WaterSensor(s, 1L, 1)
      })

    val esSinkBuilder = new ElasticsearchSink.Builder[WaterSensor](httpHosts, new ElasticsearchSinkFunction[WaterSensor] {
      override def process(element: WaterSensor, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        println("saving data: " + element)


        val json =
          new util.HashMap[String, String]()
        json.put("data", element.toString)
        val request: IndexRequest =
          Requests.indexRequest().index("ws").`type`("readingData").source(json)
        indexer.add(request)
        println("saved successfully")
      }
    })
    ds.addSink(esSinkBuilder.build())

    env.execute()



  }
}
