package com.test.OutFormat1


import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapred.HadoopOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}


/**
  *
  * @author jiale.he
  * @date 2019/04/02
  * @email jiale.he@mail.hypers.com
  */
object OutputFormatTest1 {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    // 准备数据
    val tuples: List[(String, Int)] = List(
      ("hejiale", 174),
      ("txt", 165),
      ("mousiqian", 170),
      ("zhanghan", 170),
      ("liumingdi", 168)
    )
    val data: DataSet[(String, Int)] = env.fromCollection(tuples)

    // 多路径输出的HadoopOutputFormat
    val outputFormat = new MultipleTextOutputFormat1[String, Int]()
    val job: JobConf = new JobConf()
    val outputPath = "hdfs://mini04:9000/out/flinkout1"
    FileOutputFormat.setOutputPath(job, new Path(outputPath))
    val format = new HadoopOutputFormat[String, Int](outputFormat, job)

    // 将数据输出出去
    data.output(format)

    // 出发批处理执行
    env.execute()
  }
}
