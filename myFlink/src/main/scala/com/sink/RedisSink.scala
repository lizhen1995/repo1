package com.sink

import com.testAPI.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * @Classname Redis
  * @Description TODO
  * @Date 2019/12/17 0:53
  * @Created by lz
  */
object RedisSink {
  def main(args: Array[String]): Unit = {
    //导入隐式成员
    import org.apache.flink.api.scala._
    //  创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //  从文件中读取数据
//    val inputPath = "in/SensorReading"
//    val inputDS: DataStream[String] = env.readTextFile(inputPath)

   // 集合source
        val stream1: DataStream[SensorReading] = env.fromCollection(List(
          SensorReading("sensor_1", 1547718199, 35.80018327300259),
          SensorReading("sensor_6", 1547718201, 15.402984393403084),
          SensorReading("sensor_7", 1547718202, 6.720945201171228),
          SensorReading("sensor_10", 1547718205, 38.101067604893444)
        ))

    
    val conf = new FlinkJedisPoolConfig.Builder().setHost("mydis").setPort(6379)build()
    stream1.addSink(new RedisSink[SensorReading](conf, new RedisExampleMapper))
    env.execute()
  }

}

class RedisExampleMapper extends  RedisMapper[SensorReading]{
  //定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    //把传感器的数据保存成HSET  key  filed value
    new RedisCommandDescription(RedisCommand.HSET,"senser_temperature")
  }

  override def getKeyFromData(t: SensorReading): String = t.temperature.toString

  override def getValueFromData(t: SensorReading): String = t.id
}

