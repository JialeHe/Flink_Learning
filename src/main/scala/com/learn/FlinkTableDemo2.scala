package com.learn

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}

/**
  *
  * @author jiale.he
  * @date 2019/08/29
  * @email jiale.he@mail.hypers.com
  */
object FlinkTableDemo2 {

  case class WC(word: String, count: Int)

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val input = env.fromElements("Learning Flink Tables API AND SQL").flatMap(_.split(" ")).map(WC(_, 1))
    val inputNumbers = env.fromCollection(List.range(1, 100))
    val inputTuple1 = env.fromCollection(List(("A", 1, 10), ("B", 2, 20), ("A", 3, 100), ("C", 4, 500)))
    val inoutTpule2 = env.fromCollection(List(("A", "Apple"), ("B", "bilibili")))

    // 转成table
    val tableWC: Table = input.toTable(tableEnv, 'word, 'count)

    // 注册成table
    tableEnv.registerTable("words", tableWC)
    tableEnv.registerTable("numbers", inputNumbers.toTable(tableEnv, 'num))
    tableEnv.registerTable("tuples", inputTuple1.toTable(tableEnv, 'a, 'b, 'c))
    tableEnv.registerTable("codes", inoutTpule2.toTable(tableEnv, 'code, 'name))

    // table转dataset
    tableEnv.scan("words").groupBy("word").select("word, count.sum as count").toDataSet[WC].print()

    // table转dataset
    tableEnv.scan("tuples").select("a, b as dd").toDataSet[(String, Int)].print()

    // table转dataset 别名
    tableEnv.scan("tuples").as('a1, 'a2, 'a3).select("a2,a3").toDataSet[(Int, Int)].print()

    // filter & where
    tableEnv.scan("tuples").where('a === "A").filter('b >= 3).toDataSet[(String, Int, Int)].print()


    //    env.execute("table test")
  }
}
