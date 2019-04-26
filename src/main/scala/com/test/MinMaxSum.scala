package com.test

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  *
  * @author jiale.he
  * @date 2019/03/29
  * @email jiale.he@mail.hypers.com
  */
object MinMaxSum {
  def main(args: Array[String]): Unit = {

    case class Student(name: String, age: Int, height: Double)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[Student] = env.fromElements(
      Student("hejiale", 23, 174.0),
      Student("txt", 17, 184.5),
      Student("mousiqian", 18, 174.5),
      Student("zhanghang", 16, 194.5),
      Student("chenyao", 17, 184.5),
      Student("masiyu", 18, 174.5)
    )
    val ageSum: AggregateDataSet[Student] = data.sum(1)
    val heightSum: AggregateDataSet[Student] = data.sum("height")
    println(ageSum.collect())
    println(heightSum.collect())

  }
}
