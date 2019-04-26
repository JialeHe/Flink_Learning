package com.test

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  *
  * @author jiale.he
  * @date 2019/03/29
  * @email jiale.he@mail.hypers.com
  */
object Group {
  def main(args: Array[String]): Unit = {

    case class WC(word: String, count: Long)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[WC] = env.fromElements(
      WC("aa", 20),
      WC("aa", 80),
      WC("bb", 30),
      WC("bb", 170)
    )
    // 使用自定义的reduce方法,使用key-expressions
    val res: DataSet[WC] = data.groupBy("word").reduce((w1, w2) => WC(w1.word, w1.count+w2.count))

    // 使用自定义的reduce方法,使用key-selector
    val res2 = data.groupBy(_.word).reduce((w1, w2) => WC(w1.word, w1.count+w2.count))

    println(res.collect())
    println(res2.collect())

  }
}
