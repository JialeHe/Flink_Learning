package com.test

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  *
  * @author jiale.he
  * @date 2019/04/02
  * @email jiale.he@mail.hypers.com
  */
object Parameter1 {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    // 准备工资数据
    val salary: DataSet[Double] = env.fromElements(4345.2,2123.5,5987.3,7991.2)

    // 准备补助数据
    val bouns = 1000.0


    salary.map(new MyMap(bouns)).print()

    class MyMap(bouns: Double) extends MapFunction[Double, Double]{
      override def map(salary: Double): Double = {
        // 工资 + 补助
        salary + bouns
      }
    }

  }
}
