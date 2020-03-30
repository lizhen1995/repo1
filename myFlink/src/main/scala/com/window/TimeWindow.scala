package com.window

import com.testAPI.SensorReading
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}

/**
  * @Classname TimeWindow
  * @Description TODO
  * @Date 2019/12/17 15:50
  * @Created by lz
  */
object TimeWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDS: DataStream[String] = env.socketTextStream("mydis", 8888)

    val  sensorDS: DataStream[SensorReading] = inputDS.map(line => {
      val split: Array[String] = line.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    })
    //基于时间的滚动窗口
//    val sumTempPerWindow: DataStream[(String, Double)] = sensorDS.map(r => (r.id, r.temperature))
//      .keyBy(_._1)
//      .timeWindow(Time.seconds(10))
//      .reduce((r1, r2) => (r1._1, r1._2 + r2._2))
//
//    sumTempPerWindow.print()
    //基于时间的滑动窗口
//    val sumTempPerWindow: DataStream[(String, Double)] = sensorDS
//      .map(r => (r.id, r.temperature))
//      .keyBy(_._1)
//      .timeWindow(Time.seconds(4), Time.seconds(2))
//      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))//.windowAll(EventTimeSessionWindows.withGap(Time.seconds(10)))
//
//      env.execute()
    //基于事件条数的滚动窗口
//      val minTempPerWindow: DataStream[(String, Double)] = sensorDS
//        .map(r => (r.id, r.temperature))
//        .keyBy(_._1)
//        .countWindow(5)
//        .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))
//      minTempPerWindow.print()
//        env.execute()
    //基于事件条数的滑动窗口计算
//    val keyedStream: KeyedStream[(String, Double), Tuple] = sensorDS.map(r => (r.id, r.temperature)).keyBy(0)

//      // 每当某一个 key 的个数达到 2 的时候 , 触发计算，计算最近该 key 最近 10 个元素的内容
//    val windowedStream: WindowedStream[(String, Double), Tuple, GlobalWindow] = keyedStream.countWindow(10,2)
//    val sumDstream: DataStream[(String, Double)] = windowedStream.sum(1)
//    sumDstream.print()

    val windowDS: AllWindowedStream[SensorReading, TimeWindow] = sensorDS.timeWindowAll(Time.seconds(5))
    
    env.execute()
  }

}
