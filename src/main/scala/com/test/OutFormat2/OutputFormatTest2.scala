package com.test.OutFormat2

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
object OutputFormatTest2 {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val tuples: List[(String, String)] = List(
      ("zhangsan", "120"),
      ("lisi", "123"),
      ("zhangsan", "309"),
      ("lisi", "207"),
      ("wangwu", "315")
    )

    val data: DataSet[(String, String)] = env.fromCollection(tuples)
    val outputformat = new MultipleTextOutputFormat2[String, String]()
    val job: JobConf = new JobConf()
    val outputPath = "hdfs://mini04:9000/out/flinkout2"
    FileOutputFormat.setOutputPath(job, new Path(outputPath))
    val format = new HadoopOutputFormat[String, String](outputformat, job)

    data.output(format)

    env.execute()

  }
}
