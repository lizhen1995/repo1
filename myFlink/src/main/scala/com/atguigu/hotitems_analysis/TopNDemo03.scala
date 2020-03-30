package com.atguigu.hotitems_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * requirement
  *
  * @author zhangsl
  * @date 2019/12/24 23:43
  * @version 1.0
  */


object TopNDemo03 {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = environment.readTextFile("E:\\zhangsl\\IdeaProjects\\flink_learn\\in\\apachetest.log")
    //val dataStream = environment.socketTextStream("hadoop01",8989)

    val value: WindowedStream[LogBean, (String, String), TimeWindow] = dataStream.map(line => {
      val arr: Array[String] = line.split(" ")
      val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timeStamp: Long = format.parse(arr(3).trim).getTime
      LogBean(arr(0).trim, timeStamp, arr(5).trim, arr(6).trim)
    }) // 延迟1s  允许迟到1分钟 后续设置
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LogBean](Time.seconds(1)) {
      override def extractTimestamp(element: LogBean): Long = {
        element.time
      }
    })

      .keyBy(x => (x.url, x.ip)) // (地区，商品id   ， count)
      //.keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
    value
        //.allowedLateness(Time.seconds(60))
        .aggregate( new MyAgg1() , new MyWindow1() )
      //.print()

//      .keyBy(_.WindowTime)
        .process(new MyProcessUrl2(3))
        .print()
    environment.execute("hot url")
  }
}

class MyAgg1() extends AggregateFunction[LogBean,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: LogBean, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}



//class MyWindow1() extends WindowFunction[Long,WindowEnv,String,TimeWindow]{
//  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[WindowEnv]): Unit = {
//    out.collect( WindowEnv(key,window.getEnd,input.iterator.next()) )
//  }
//}

class MyWindow1() extends WindowFunction[Long,WindowEnv,(String,String),TimeWindow]{
  override def apply(key: (String,String), window: TimeWindow, input: Iterable[Long], out: Collector[WindowEnv]): Unit = {
    out.collect( WindowEnv(key._1+"-"+key._2,window.getEnd,input.iterator.next()) )
  }
}

class MyProcessUrl1(n:Int) extends KeyedProcessFunction[Long,WindowEnv,String] {

  lazy val urlState: ListState[WindowEnv] = getRuntimeContext.getListState(new ListStateDescriptor[WindowEnv]("url-state",classOf[WindowEnv]))


  override def processElement(value: WindowEnv, ctx: KeyedProcessFunction[Long, WindowEnv, String]#Context, out: Collector[String]): Unit = {
    //将状态假如到listState中
    urlState.add(value)
    // 注册定时器   每次注册覆盖前面的定时器(猜的)
    ctx.timerService().registerEventTimeTimer(value.WindowTime + 2)  // 延迟1s触发定时器
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, WindowEnv, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 创建一个listBuffer 接收定时器
    val list: ListBuffer[WindowEnv] = new ListBuffer[WindowEnv]
    // 循环listState 将状态加入到list
    val iter: util.Iterator[WindowEnv] = urlState.get().iterator()
    while (iter.hasNext) {
      list.append(iter.next())
    }


    // 对list排序取topN
    val topn: ListBuffer[WindowEnv] = list.sortWith(_.count>_.count).take(n)

    var result:StringBuilder = new StringBuilder

    result.append("============================").append("\n")
      .append("时间:").append(new Timestamp( timestamp - 1 )).append("\n")
      for (i <- topn.indices) { // indices 获取下标 从0开始
        result.append("NO").append(i+1).append(": ")
          .append("  url=").append(topn(i).url)
          .append("  count=").append(topn(i).count).append("\n")
      }

    result.append("===============================").append("\n")

    // 控制一下输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }

}

class MyProcessUrl2(i: Int) extends ProcessFunction[WindowEnv,String]{
  lazy val urlState: ListState[WindowEnv] = getRuntimeContext.getListState(new ListStateDescriptor[WindowEnv]("url-state",classOf[WindowEnv]))
  override def processElement(value: WindowEnv, ctx: ProcessFunction[WindowEnv, String]#Context, out: Collector[String]): Unit = {
    //将状态假如到listState中
    urlState.add(value)
    // 注册定时器   每次注册覆盖前面的定时器(猜的)
    ctx.timerService().registerEventTimeTimer(value.WindowTime + 2)  // 延迟1s触发定时器
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[WindowEnv, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 创建一个listBuffer 接收定时器
    val list: ListBuffer[WindowEnv] = new ListBuffer[WindowEnv]
    // 循环listState 将状态加入到list
    val iter: util.Iterator[WindowEnv] = urlState.get().iterator()
    while (iter.hasNext) {
      list.append(iter.next())
    }


    // 对list排序取topN
    val topn: ListBuffer[WindowEnv] = list.sortWith(_.count>_.count).take(i)

    var result:StringBuilder = new StringBuilder

    result.append("============================").append("\n")
      .append("时间:").append(new Timestamp( timestamp - 1 )).append("\n")
    for (i <- topn.indices) { // indices 获取下标 从0开始
      result.append("NO").append(i+1).append(": ")
        .append("  url=").append(topn(i).url)
        .append("  count=").append(topn(i).count).append("\n")
    }

    result.append("===============================").append("\n")

    // 控制一下输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}

case class LogBean(ip:String,time:Long,method:String,url:String)
case class WindowEnv(url:String,WindowTime: Long,count:Long)
