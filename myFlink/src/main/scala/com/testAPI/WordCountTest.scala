package com.testAPI

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


/**
  * Description：WordCount案例  无界流演示（使用无界流的api去分析处理离线的数据）<br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020/2/2618:29
  *
  * @author 张嵩玲
  * @version : 1.0
  */
object WordCountTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    environment.setParallelism(1)

    val DStream1 = environment.socketTextStream("mydis",9999)//.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
//      override def extractTimestamp(s: String) = {
//        val split: Array[String] = s.split(",")
//        split(1).toLong
//      }
//    });

    val DS1: DataStream[(String, Int)] = DStream1.map(x => {
      println(x)
      (x, 1)
    })
    val DStream2 = environment.socketTextStream("mydis",8888)//fromElements("a,1","b,1","c,1")//.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
//      override def extractTimestamp(s: String) = {
//        val split: Array[String] = s.split(",")
//        split(1).toLong
//      }
//    });
    val DS2: DataStream[(String, Int)] = DStream2.map(x => {
      println(x)
      (x, 1)
    })

    DS1.join(DS2)
      .where(x=>x._1)
      .equalTo(x=>x._1)
      // TumblingEventTimeWindows
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .apply(new myJoin())
        .print()

//    import org.apache.flink.api.scala._
//    DStream.flatMap(_.split("\\s+"))
//      .map((_,1))
//      .keyBy(0)
//      .sum(1)
//      .print("WordCount Result--->")

    environment.execute()
  }
}

class myJoin() extends JoinFunction[(String, Int),(String, Int),String]{
  override def join(first: (String, Int), second: (String, Int)): String = {
    first._1+"-"+first._2+"-"+second._1+"-"+second._2
  }
}
