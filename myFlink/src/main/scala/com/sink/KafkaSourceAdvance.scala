package com.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @Classname KafkaSourceAdvance
  * @Description TODO
  * @Date 2019/12/19 16:23
  * @Created by lz
  */
object KafkaSourceAdvance {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //对接kafka
        val properties: Properties  = new Properties()
        properties.setProperty("bootstrap.servers", "mydis:9092")
        properties.setProperty("group.id", "consumer-group11")
        // 设置隔离级别
        properties.setProperty("isolation.level","read_committed");
        // 关闭自动提交
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("session.timeout.ms", "30000");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")
        val stream3: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("testFlinkSink2", new SimpleStringSchema(), properties))
        stream3.print().setParallelism(1)
    //stream3
    env.execute()
  }
}
