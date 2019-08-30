package com.learn

import com.utils.CommonUtils
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.types.Row
import org.apache.flink.api.scala._

/**
  *
  * @author jiale.he
  * @date 2019/08/29
  * @email jiale.he@mail.hypers.com
  */
object FlinkTableDemo1 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取TableEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    // 转化DataStream为Table
    val stream: DataStream[String] = CommonUtils.getDataStream(env, "table")
    val table1: Table = tableEnv.fromDataStream(stream)

    // 转化Table为DataStream
    val row: DataStream[Row] = tableEnv.toAppendStream[Row](table1)
    val retractStream: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](table1)


  }
}
