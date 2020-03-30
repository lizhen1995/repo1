package com.baseDemo

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object Bluuuuuue {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val kafkaDS: DataStream[String] = env.socketTextStream("mydis" ,7777)
    import org.apache.flink.streaming.api.scala._
    val logDS: DataStream[LogData] = kafkaDS.map { line =>
      val arr: Array[String] = line.split("\t")
      LogData(arr(0), arr(1), arr(2), arr(3).toLong, arr(4), arr(5))
    }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LogData](Time.seconds(1)) {
      override def extractTimestamp(element: LogData): Long = element.time
    })

    logDS.timeWindowAll(Time.seconds(5))
        .aggregate(new MyAggre).print()

//    logDS.keyBy(_.dt)
//      .timeWindow(Time.minutes(5))
//      .process(new MyCountProcess)

    env.execute()
  }
}

case class LogData(dt:String,
                     logkey:String,
                     clientip:String,
                     time:Long,
                     logid:String,
                     userid:String
                    )

class MyCountProcess extends ProcessWindowFunction[LogData,Long,String,TimeWindow]{
  lazy val lastCount: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("logCount",classOf[Long]))

  override def process(key: String, context: Context, elements: Iterable[LogData], out: Collector[Long]): Unit = {
    val tempCount: Long = lastCount.value()
    val count: Long = tempCount+elements.toList.length
    lastCount.update(count)
//    if( new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.currentWatermark) ???) lastCount.clear()
    out.collect(count)
  }
}

class MyAggre extends AggregateFunction[LogData,Long,Long] {
  override def createAccumulator(): Long = 0

  override def add(value: LogData, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}




