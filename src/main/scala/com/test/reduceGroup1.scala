package com.test

import akka.stream.impl.fusing.Collect
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  *
  * @author jiale.he
  * @date 2019/03/29
  * @email jiale.he@mail.hypers.com
  */
object reduceGroup1 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[(Int, String)] = env.fromElements(
      (20,"zhangsan"),
      (22,"zhangsan"),
      (22,"lisi"),
      (20,"zhangsan")
    )

    val res: DataSet[(Int, String)] = data.groupBy(1).reduceGroup {
      //将相同的元素用set去重
      (in, out: Collector[(Int, String)]) =>
        in.toSet foreach out.collect
    }
    println(res.collect())
  }
}
