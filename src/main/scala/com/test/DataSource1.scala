package com.test

import org.apache.flink.api.scala._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  *
  * @author jiale.he
  * @date 2019/03/29
  * @email jiale.he@mail.hypers.com
  */
object DataSource1 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //0. 用element创建DataSet(fromElement)
    val ds0: DataSet[String] = env.fromElements("spark", "flink")
    ds0.print()

    //1. 用Tuple创建DataSet(fromElement)
    val ds1: DataSet[(Int, String)] = env.fromElements((1, "spark"), (2, "flink"))
    ds1.print()

    //2. 用Array创建DataSet
    val ds2: DataSet[String] = env.fromCollection(Array("spark", "flink"))
    ds2.print()

    //3. 用ArrayBuffer创建DataSet
    val ds3: DataSet[String] = env.fromCollection(ArrayBuffer("spark", "flink"))
    ds3.print()

    //4. 用List
    env.fromCollection(List("spark", "flink"))

    //5. Vector
    env.fromCollection(Vector("spark", "flink"))

    //6. Queue
    env.fromCollection(mutable.Queue("spark", "flink"))

    //7. Stack
    env.fromCollection(mutable.Stack("spark", "flink"))

    //8. 

  }
}
