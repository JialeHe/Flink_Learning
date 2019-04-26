package com.test.OutFormat1

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
  * 自定义OutputFormat
  * @author jiale.he
  * @date 2019/04/02
  * @email jiale.he@mail.hypers.com
  */
class MultipleTextOutputFormat1[K, V] extends MultipleTextOutputFormat[K, V]{

  /**
    * 用于产生文件名称
    * 将Key作为文件名
    * @param key DataSet的Key
    * @param value DataSet的Value
    * @param name DataSet的Partition Id (从1开始)
    * @return 文件名
    */
  override def generateFileNameForKeyValue(key: K, value: V, name: String): String =
    key.asInstanceOf[String]

  /**
    * 用于产生文件内容中的Key
    * 这里文件内容的Key就是DataSet的Key
    * @param key DataSet的Key
    * @param value DataSet的Value
    * @return file的Key
    */
  override def generateActualKey(key: K, value: V): K =
    NullWritable.get().asInstanceOf[K]

  /**
    * 产生文件内容中的Value
    * 这里文件内容中的value就是DataSet的Value
    * @param key DataSet的Key
    * @param value DataSet的Value
    * @return file的Value
    */
  override def generateActualValue(key: K, value: V): V =
    value.asInstanceOf[V]
}
