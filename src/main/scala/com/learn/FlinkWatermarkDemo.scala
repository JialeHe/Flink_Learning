package flink.watermark

import java.text.SimpleDateFormat

import com.utils.CommonUtils
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Flink的watermark测试
  */
object FlinkWaterMarkDemo {
  def main(args: Array[String]): Unit = {
    // 设置streaming环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取checkpoint
    val checkpoint = env.getCheckpointConfig
    // 设置任务取消或者失败的时候,不会自动清除checkpoint
    checkpoint.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置时间Time Notion为eventtime默认使用的是ProcessTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 开启checkpoint,10秒钟一次
    env.enableCheckpointing(10000)
    // 设置在checkpoint失败的时候,任务会继续运行
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 设置同一时间只运行1个checkpoint运行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //设置checkpoint必须要在20秒内完成,否则的话就丢弃
    env.getCheckpointConfig.setCheckpointTimeout(20000)
    // 设置checkpoint语义是exactly-once的
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 规定在两次checkpoints之间的最小时间是为了流应用可以在此期间有明显的处理进度。比如这个值被设置为5秒，
    // 则在上一次checkpoint结束5秒之内不会有新的checkpoint被触发。这也通常意味着checkpoint interval的值会比这个值要大。
    // 为什么要设置这个值？因为checkpiont interval有时候会不可靠，比如当文件系统反应比较慢的时候，checkpiont花费的时间可能就比预想的要多，
    // 这样仅仅只有checkpoint interval的话就会重叠。记住，设置minimum time between checkpoints也要求checkpoints的并发度是1
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 设置Flink的重启策略,重试4次,每次间隔10秒
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4,10000))
    env.setParallelism(1)
    //从kafka中读取数据,得到datastream;
    val dataStream = CommonUtils.getDataStream(env = env,"test")
    val filter_stream = dataStream
      .filter(_.nonEmpty)
      .filter(_.contains(","))
      .map(x => {
        val arr = x.split(",")
        val code = arr(0)
        val time = arr(1).toLong
        (code, time)
      })
      // 指派时间戳,并生成WaterMark
      .assignTimestampsAndWatermarks(new TimestampExtractor)
      .keyBy(_._1)
      //keyed stream window
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new MyWindowFunctionTest1)
      .print()
    env.execute("watermark test")
  }
}

/**
  * 生成watermark
  */
class TimestampExtractor extends AssignerWithPeriodicWatermarks[(String, Long)] with Serializable {
  var currentMaxTimestamp = 0L
  //最大允许的乱序时间是
  val maxOutOfOrderness = 10000L

  var wm: Watermark = null
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")

  //获取时间戳(先执行的)
  override def extractTimestamp(t: (String, Long), l: Long): Long = {
    //数据里面的时间戳
    val timestamp = t._2
    //获取当前最大时间戳和enentTime里面的时间戳的最大值
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    println("key:"+t._1 +" |timestamp:"+t._2 +" |时间戳格式化后:"+sdf.format(t._2) +" |currentMaxTimestamp:" +sdf.format(currentMaxTimestamp) +" |watermark:" +wm.getTimestamp)
    timestamp
  }

  //生成watermark,其实就是一个时间戳,而且是上一条数据的watermark的时间,第一条数据的watermark是-10000
  override def getCurrentWatermark: Watermark = {
    wm = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    wm
  }
}

/**
  *  IN The type of the input value.
  *  OUT The type of the output value.
  *  KEY The type of the key.
  */
class MyWindowFunctionTest1 extends WindowFunction[(String, Long), (String, Int, String, String, String, String), String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int, String, String, String, String)]): Unit = {
    //输入参数是一个迭代器,根据时间戳排序
    val list = input.toList.sortBy(_._2)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    //输出
    out.collect( key, input.size, format.format(list.head._2), format.format(list.last._2), format.format(window.getStart), format.format(window.getEnd))
  }
}