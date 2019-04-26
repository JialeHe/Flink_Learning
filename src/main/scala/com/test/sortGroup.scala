package com.test


import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.table.expressions.Collect
import org.apache.flink.util.Collector

/**
  *
  * @author jiale.he
  * @date 2019/03/29
  * @email jiale.he@mail.hypers.com
  */
object sortGroup {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[(Int, String)] = env.fromElements(
      (20, "zhangsan"),
      (22, "zhangsan"),
      (22, "lisi"),
      (22, "lisi"),
      (22, "aa"),
      (18, "zhangsan"),
      (18, "zhangsan")
    )
    val res: DataSet[(Int, String)] = data
      .groupBy(0)
      // 用string进行组内数据排序
      .sortGroup(1, Order.ASCENDING)
      // 对组内数据进行reduce处理
      .reduceGroup({
      (in, out: Collector[(Int, String)]) => in.toSet foreach out.collect
    })
    println(res.collect())

  }
}
