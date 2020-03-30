package com.sink

import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.util.serialization.{KeyedSerializationSchema, KeyedSerializationSchemaWrapper}

/**
  * @Classname KafkaSinkTranscation
  * @Description TODO
  * @Date 2019/12/19 14:16
  * @Created by lz
  */
object KafkaSinkTransaction {
  def main(args: Array[String]): Unit = {
    // flatMap 和 Map 需要引用的隐式转换
    import org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)

    //该监测点测试效果为，程序重试3次，每次间隔50秒，三次不成功，程序会failed，在重试过程中监测点是不会更新的
    // 也就是说，监测点这时候是保存着最近计算的正确状态的，当再次重启是，如果指定了监测点的metadata文件，就从读取那个文件
    //里面的状态，然后继续运行，checkpoint中只保留某一个时刻（某一条数据全部执行完毕后）的状态。
    //注：感觉flink整合kafka的时候，偏移量主要是有kafka维护，checkpoint只是防止意外出错恢复用的
    //当出错后，读取偏移量里面的状态，读取到source里的偏移量，带着偏移量去卡发卡消费，正常情况下，应该是不带偏移量去
    //访问kafka的，利用kafak内部维护的偏移量，如果出错了，才读取监测点状态，拉去特定位置数据。
    env.enableCheckpointing(10000)//10秒一个checkpoint
    env.setStateBackend(new FsStateBackend("file:///E:/myFlink/out/checkpoint"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)//指定处理的时间特性
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(50)))//重启策略RestartStrategies.fixedDelayRestart(3,Time.seconds(100))，会在100秒内重试3次，失失败事务中到开始到错误数据段的数据
    //env.setRestartStrategy(RestartStrategies.fallBackRestart())//重启策略 RestartStrategies.fallBackRestart()失败后立即重启，丢失失败事务中到开始到错误数据段的数据
    //env.setRestartStrategy(RestartStrategies.noRestart())//重启策略 RestartStrategies.fallBackRestart()失败后立即重启，丢失失败事务中到开始到错误数据段的数据
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//确保一次语义
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)//退出不删除checkpoint
    // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
    env.getCheckpointConfig.setFailOnCheckpointingErrors(true)


     //env.setStateBackend(new FsStateBackend("file:///E:/myFlink/out/checkpoints"))
//      接收 socket 文本流
//    对接kafka
        val properties: Properties = new Properties()
        properties.setProperty("bootstrap.servers", "mydis:9092")
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("group.id", "consumer-group164")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset", "latest")
        val textDstream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("test3", new SimpleStringSchema(), properties))
        textDstream.print()
    val dataStream: DataStream[(String, Int)] =
      textDstream.flatMap(_.split("\\s")).filter(_.nonEmpty).map(t=>(t.toInt.toString,1)).keyBy(0).sum(1)

    val res: DataStream[String] = dataStream.map(t=>{
      println("map输出："+t._1+t._2)
      ""+t._1+t._2
    })

    //这里实现kafka到flink到kafka的精准一次性消费需要保证flink开启checkpoint，保证flink内部执行精准无误
    //还需要才FlinkKafkaProducer011的构造中添加semantic: FlinkKafkaProducer011.Semantic
    //注意kafka的事务隔离级别需要改成读已提交，kafka事务默认超时是15分钟，flink的保存checkpoint
    // 这个动作最长是一个小时，这里面就会有问题，checkpoint保存成功了，kafka事务失效了，所以需要注意
    //修改时间
    // inputDS.addSink(new FlinkKafkaProducer011[String]("mydis:9092",
    // "testFlinkSink", new SimpleStringSchema()))
    val props: Properties = new Properties()
    // bootstrap.servers是Kafka集群的IP地址。多个时,使用逗号隔开
    props.put("bootstrap.servers", "mydis:9092")
    // 设置事务id
    props.put("transactional.id", "first-transactional")
    // 设置幂等性
    props.put("enable.idempotence", "true")
    //Set acknowledgements for producer requests.
    props.put("acks", "all")
    //If the request fails, the producer can automatically retry,
    props.put("retries", "1")
    //Specify buffer size in config,这里不进行设置这个属性,如果设置了,还需要执行producer.flush()来把缓存中消息发送出去
    //props.put("batch.size", 16384);
    //Reduce the no of requests less than 0
    props.put("linger.ms", "1")
    //The buffer.memory controls the total amount of memory available to the producer for buffering.
    props.put("buffer.memory", "33554432")
    props.put("transaction.timeout.ms", "400000")
    // Kafka消息是以键值对的形式发送,需要设置key和value类型序列化器
//  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //(KeyedSerializationSchema)(new KeyedSerializationSchemaWrapper(serializationSchema)
    val schema: KeyedSerializationSchema[String] = new KeyedSerializationSchemaWrapper(new SimpleStringSchema()).asInstanceOf[KeyedSerializationSchema[String]]
    val producer: FlinkKafkaProducer011[String] = new FlinkKafkaProducer011[String](
      "testFlinkSink2",
      schema,
      props,
      Semantic.EXACTLY_ONCE
    )
    res.addSink(producer)
    env.execute()

  }

}
