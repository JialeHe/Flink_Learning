package com.test

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._

/**
  *
  * @author jiale.he
  * @date 2019/04/02
  * @email jiale.he@mail.hypers.com
  */
object Parameter2 {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    // 准备worker数据
    case class Worker(name: String, salary:Double)
    val workers: DataSet[Worker] = env.fromElements(
      Worker("hejiale", 1356.67),
      Worker("txt", 1476.67)
    )

    // 工作月份,作为参数传递
    val month = 4

    workers.map(new MyMap(month)).print()

    class MyMap(month: Int) extends MapFunction[Worker, Worker]{
      override def map(worker: Worker): Worker = {
        // 取出参数,取出Worker进行计算
        Worker(worker.name, worker.salary * month)
      }
    }

  }
}
