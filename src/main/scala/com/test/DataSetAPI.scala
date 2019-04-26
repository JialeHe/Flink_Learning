package com.test

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  *
  * @author jiale.he
  * @date 2019/03/29
  * @email jiale.he@mail.hypers.com
  */
object DataSetAPI {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    // Int
    val data: DataSet[Int] = env.fromElements(2,5,9,8,7,3)
    val res = data.reduce(_+_)
    res.print()

    // String
    val data2 = env.fromElements("zhangsan boy", " lisi girl")
    val res2 = data2.reduce(_+_)
    res2.print()

  }
}