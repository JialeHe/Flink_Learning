package com.test
import org.apache.flink.api.scala._
/**
  *
  * @author jiale.he
  * @date 2019/03/29
  * @email jiale.he@mail.hypers.com
  */
object Groups {
  def main(args: Array[String]): Unit = {

    case class Student(val name: String, addr: String, salary: Double)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data:DataSet[Student] = env.fromElements(
      Student("lisi","shandong",2400.00),
      Student("zhangsan","henan",2600.00),
      Student("lisi","shandong",2700.00),
      Student("lisi","guangdong",2800.00)
    )

    val res1: DataSet[Student] = data.groupBy("name", "addr").reduce((s1, s2) =>
      Student(s1.name + "-" + s2.name, s1.addr + "-" + s2.addr, s1.salary + s2.salary)
    )
    println(res1.collect())

  }
}
