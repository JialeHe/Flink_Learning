package com.test.OutFormat2

import java.util.Date

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
  *
  * @author jiale.he
  * @date 2019/04/02
  * @email jiale.he@mail.hypers.com
  */
class MultipleTextOutputFormat2[K, V] extends MultipleTextOutputFormat[K, V] {
  override def generateFileNameForKeyValue(key: K, value: V, name: String): String =
    key+"_"+new Date().getTime

  override def generateActualKey(key: K, value: V): K =
    key.asInstanceOf[K]

  override def generateActualValue(key: K, value: V): V =
    value.asInstanceOf[V]
}
