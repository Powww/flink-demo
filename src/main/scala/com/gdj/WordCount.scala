package com.gdj

import org.apache.flink.api.scala._
/**
 *
 *批处理WordCount
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val fileDS: DataSet[String] = env.readTextFile("input/word.txt")
    val wordDS: DataSet[String] = fileDS.flatMap(_.split(" "))
    val wordToOneDS: DataSet[(String, Int)] = wordDS.map((_, 1))
    val resultDS: AggregateDataSet[(String, Int)] = wordToOneDS.groupBy(0).sum(1)
    resultDS.print()
  }
}
