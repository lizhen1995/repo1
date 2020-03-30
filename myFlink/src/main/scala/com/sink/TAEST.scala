package com.sink

import java.util.Properties

import com.testAPI.{FreezingAlert, SensorReading}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @Classname TAEST
  * @Description TODO
  * @Date 2019/12/19 17:07
  * @Created by lz
  */
object TAEST {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    env.enableCheckpointing(1000)//10秒一个checkpoint
    env.setStateBackend(new FsStateBackend("file:///E:/myFlink/out/checkpoints"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)//指定处理的时间特性
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(100)))//重启策略
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//确保一次语义
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)//退出不删除checkpoint

    // env.setStateBackend(new FsStateBackend("file:///E:/myFlink/out/checkpoints/074651996ccc748df56708beb2e7eaf5"))
    //  接收 socket 文本流
    val textDstream: DataStream[String] = env.socketTextStream("mydis", 9888)
    //textDstream.print()
    // flatMap 和 Map 需要引用的隐式转换
    import org.apache.flink.api.scala._
    val  sensorDS: DataStream[SensorReading] = textDstream.map(line => {
      val split: Array[String] = line.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    })
//    val keyed: KeyedStream[SensorReading, String] = sensorDS.keyBy(_.id).flatMapWithState()
//    keyed.process(new myProsess()).print()

    env.execute()

  }

}

class myProsess extends ProcessFunction[SensorReading,SensorReading]{


  override def open(parameters: Configuration): Unit = {
    println("open执行")
  }

  override def close(): Unit = {
    println("close执行")
  }

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if( value.temperature < 32.0 ){
      ctx.output( new OutputTag[String]( "freezing alert" ), "freezing alert for " + value.id )
    }
    out.collect( value )
  }}
