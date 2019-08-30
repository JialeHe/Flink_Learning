package com.learn

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  * @author jiale.he
  * @date 2019/08/30
  * @email jiale.he@mail.hypers.com
  */
object FlinkStreamTable {

  // 自定义类
  case class Person(name: String, age: Int, city: String)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 1000L))

  }
}
