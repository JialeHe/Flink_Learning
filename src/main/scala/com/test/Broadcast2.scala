package com.test


import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  *
  * @author jiale.he
  * @date 2019/04/02
  * @email jiale.he@mail.hypers.com
  */
object Broadcast2 {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    env.registerCachedFile("hdfs://mini04:9000/data/workcount.txt", "testFile")

    // 准备人工数据
    case class Worker(name: String, salaryPerMonth: Double)
    val workers: DataSet[Worker] = env.fromElements(
      Worker("hejiale", 1356.67),
      Worker("txt", 1476.67),
      Worker("msq", 222)
    )

    workers.map(new MyMapper()).print()


    // 使用缓存数据和人工数据做计算
    class MyMapper() extends RichMapFunction[Worker, Worker]{
      private var lineList: ListBuffer[String] = new ListBuffer[String]
      // 在open方法中获取缓存文件
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        // 获取文件
        val file: File = getRuntimeContext.getDistributedCache.getFile("testFile")
        // 按行 读取文件 存入迭代器
        val lines: Iterator[String] = Source.fromFile(file.getAbsoluteFile).getLines()
        // 遍历迭代器, append入集合
        lines.foreach(line => {
          this.lineList.append(line)
        })
      }

      // 在map方法中使用获取到的缓存文件的内容
      override def map(worker: Worker): Worker = {
        var name: String = ""
        var month: Int = 0
        // 遍历集合
        for (lineStr <- this.lineList) {
          // 切分
          val fields: Array[String] = lineStr.split(":")
          if(fields.length == 2) {
            name = fields(0).trim
            // 匹配姓名 获得月数
            if (name.equalsIgnoreCase(worker.name)){
              month = fields(1).trim.toInt
            }
          }
          if (name.nonEmpty && month > 0){
            return Worker(worker.name, worker.salaryPerMonth * month)
          }
        }
        Worker(worker.name, worker.salaryPerMonth * month)
      }
    }

  }
}

