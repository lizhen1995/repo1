package com.baseDemo

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @Classname TestRichMapFunction
  * @Description TODO
  * @Date 2020/3/5 0:01
  * @Created by lz
  */
object TestRichMapFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //  接收 socket 文本流
    val textDstream: DataStream[String] =env.socketTextStream("mydis", 8888)
    // flatMap 和 Map 需要引用的隐式转换
    import org.apache.flink.api.scala._
    val dataStream: DataStream[(String, Int)] =
      textDstream.flatMap(new myFlatMapFunction).filter(_.nonEmpty).map((_,1))
       // .keyBy(0).sum(1)
    //dataStream.process(new myWindow())
    dataStream.print().setParallelism(1)
  }
}

class myFlatMapFunction extends RichFlatMapFunction[String,String]{


  override def open(parameters: Configuration): Unit = {

  }

  override def flatMap(value: String, out: Collector[String]): Unit = {

  }
}
