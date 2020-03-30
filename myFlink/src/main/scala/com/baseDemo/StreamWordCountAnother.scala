package com.baseDemo

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @Classname StreamWordCount
  * @Description TODO
  * @Date 2019/12/16 9:55
  * @Created by lz
  */
object StreamWordCountAnother {
  def main(args: Array[String]): Unit = {
    //  从外部命令中获取参数

    //  创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    //  接收 socket 文本流
    val textDstream: DataStream[String] = env.socketTextStream("mydis", 7777)
    // flatMap 和 Map 需要引用的隐式转换
    import org.apache.flink.api.scala._
    val dataStream: DataStream[(String, Int)] =
      textDstream.flatMap(_.split("\\s")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    dataStream.print().setParallelism(1)
    //  启动 executor ，执行任务
    env.execute("Socket stream word count another")
  }

}
