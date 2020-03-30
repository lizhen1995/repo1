package com.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic

/**
  * @Classname KafkaSink
  * @Description TODO
  * @Date 2019/12/17 0:23
  * @Created by lz
  */
object KafkaSink {
  def main(args: Array[String]): Unit = {

    //导入隐式成员
    import org.apache.flink.api.scala._
    //  创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //  从文件中读取数据
    val inputPath = "in/SensorReading"
    val inputDS: DataStream[String] = env.readTextFile(inputPath)

    //这里实现kafka到flink到kafka的精准一次性消费需要保证flink开启checkpoint，保证flink内部执行精准无误
    //还需要才FlinkKafkaProducer011的构造中添加semantic: FlinkKafkaProducer011.Semantic
    //注意kafka的事务隔离级别需要改成读已提交，kafka事务默认超时是15分钟，flink的保存checkpoint
    // 这个动作最长是一个小时，这里面就会有问题，checkpoint保存成功了，kafka事务失效了，所以需要注意
    //修改时间
    val producer = new FlinkKafkaProducer011[String]("mydis:9092",
      "testFlinkSink", new SimpleStringSchema())

    //inputDS.addSink(producer)


    env.execute()

  }

}

