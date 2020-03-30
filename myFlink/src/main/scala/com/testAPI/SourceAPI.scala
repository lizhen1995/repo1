package com.testAPI

import java.util.{Properties, Random}

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

/**
  * @Classname SourceAPI
  * @Description TODO
  * @Date 2019/12/16 14:24
  * @Created by lz
  */

case class SensorReading(id:String , time:Long,  temperature:Double)

object SourceAPI {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(100)//5秒一个checkpoint
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)//指定处理的时间特性
    env.setRestartStrategy(RestartStrategies.noRestart())//重启策略
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//确保一次语义

    val checkPointPath = new Path("file:///E:/myFlink/out/checkpoints")//fs状态后端配置,如为file:///,则在taskmanager的本地
    val fsStateBackend: StateBackend= new FsStateBackend(checkPointPath)
    env.setStateBackend(fsStateBackend)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)//退出不删除checkpoint

    //从监测点中恢复数据
    //env.setStateBackend(fsStateBackend)

    //集合source
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))
//    stream1.print("stream1:").setParallelism(1)

      //读文本
    val stream2: DataStream[String] = env.readTextFile("in/SensorReading")
    stream2.print("stream2:")
    env.execute()

    //更松散的读取范式
//    env.fromElements(1,2.0,"hello flink").print()
//    env.execute()

    //对接kafka
//    val properties: Properties = new Properties()
//    properties.setProperty("bootstrap.servers", "mydis:9092")
//    properties.setProperty("group.id", "consumer-group1")
//    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("auto.offset.reset", "latest")
//    val stream3: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("testFlinkSink", new SimpleStringSchema(), properties))
//    stream3.print().setParallelism(1)
    //stream3

    //自定义source
//    env.addSource(new MySensorSource).print()
    //env.execute()

  }

}
class MySensorSource extends SourceFunction[SensorReading]{
  // flag:  表示数据源是否还在正常运行
  var running: Boolean = true
  override def cancel(): Unit = {
    running = false
  }
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit
  = {
    //  初始化一个随机数发生器
    val rand = new Random()
    var curTemp = 1.to(10).map(
      i => ( "sensor_" + i, 65 + rand.nextGaussian() * 20 )
    )
    while(running){
      //  更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian() )
      )
      //  获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t => ctx.collect(SensorReading(t._1, curTime, t._2))
      )
      Thread.sleep(100)
    }
  }
}

