package com.baseDemo

import com.sink.RedisExampleMapper
import com.testAPI.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import project.pojo.PayResult

/**
  * @Classname StreamWordCount
  * @Description TODO
  * @Date 2019/12/16 9:55
  * @Created by lz
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //  从外部命令中获取参数

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    env.enableCheckpointing(1000)//5秒一个checkpoint
    env.setStateBackend(new FsStateBackend("hdfs://mydis:9000/flink/checkpoint"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)//指定处理的时间特性
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(100)))//重启策略
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//确保一次语义
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)//退出不删除checkpoint
    // env.setStateBackend(new FsStateBackend("file:///E:/myFlink/out/checkpoints/074651996ccc748df56708beb2e7eaf5"))
    //  接收 socket 文本流
    val textDstream: DataStream[String] = env.socketTextStream("mydis", 9888)
    // flatMap 和 Map 需要引用的隐式转换
    import org.apache.flink.api.scala._
    val dataStream: DataStream[(String, Int)] =
      textDstream.flatMap(_.split("\\s")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    //dataStream.process(new myWindow())
    dataStream.print().setParallelism(1)
    val conf = new FlinkJedisPoolConfig.Builder().setHost("mydis").setPort(6379)build()
    dataStream.addSink(new RedisSink[(String, Int)](conf, new RedisExampleMapper1))
    //  启动 executor ，执行任务
    env.execute("Socket sttream word count")
  }
  class RedisExampleMapper1 extends  RedisMapper[(String, Int)]{
    override def getCommandDescription: RedisCommandDescription =  //把传感器的数据保存成HSET  key  filed value
      new RedisCommandDescription(RedisCommand.HSET,"wordCountByZSL")

    override def getKeyFromData(t: (String, Int)): String =t._1

    override def getValueFromData(t: (String, Int)): String = t._2.toString
  }

}

//class myWindow() extends ProcessWindowFunction[PayResult]()


