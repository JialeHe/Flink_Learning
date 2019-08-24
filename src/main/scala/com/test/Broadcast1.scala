package com.test

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  *
  * @author jiale.he
  * @date 2019/04/02
  * @email jiale.he@mail.hypers.com
  */
object Broadcast1 {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    // 准备数据
    case class Worker(name: String, salary: Double)
    val workers: DataSet[Worker] = env.fromElements(
      Worker("hejiale", 13320.14),
      Worker("txt", 10540.87),
      Worker("msq", 12000.0)
    )

    // 准备统计数据 用于广播
    case class Count(name: String, month: Int)
    val counts: DataSet[Count] = env.fromElements(
      Count("hejiale", 12),
      Count("txt", 10)
    )

    workers.map(new RichMapFunction[Worker, Worker] {
      private var cwork: java.util.List[Count] = null

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        // 访问广播数据
        cwork = getRuntimeContext.getBroadcastVariable[Count]("countWorkInfo")
      }

      override def map(w: Worker): Worker = {
        // 解析广播数据
        var i = 0
        while (i < cwork.size()) {
          val c: Count = cwork.get(i)
          i += 1
          if (c.name.equalsIgnoreCase(w.name)) {
            // 有相应的信息
            return Worker(w.name, w.salary * c.month)
          }
        }
        // 无匹配的值
        Worker(w.name, 0.0)
      }
    }).withBroadcastSet(counts, "countWorkInfo").print()
  }
}
