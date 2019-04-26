package com.hjl

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
/**
  *
  * @author jiale.he
  * @date 2019/03/04
  * @email jiale.he@mail.hypers.com
  */
object flinkWordcount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment(1)
    val lines: DataSet[String] = env.readTextFile("C:\\kms10.log")
    val word: AggregateDataSet[(String, Int)] = lines
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    word.print()
    //word.writeAsText("hdfs://mini04:9000/flinkTet")
    env.execute("flink wordcount demo")
  }
}


