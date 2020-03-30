package com.testAPI

import java.util

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.{CheckpointedFunction, ListCheckpointed}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @Classname KedProcessFun
  * @Description TODO
  * @Date 2020/3/9 11:47
  * @Created by lz
  */
object KedProcessFun {
  def main(args: Array[String]): Unit = {

//    val params = ParameterTool.fromArgs(args)
//    val host: String = params.get("host")
//    val port: Int = params.getInt("port")

    // 创建一个流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(4)
    //    env.disableOperatorChaining()

    // 接收socket数据流
    val textDataStream = env.socketTextStream("mydis", 9999)

    // 逐一读取数据，分词之后进行wordcount
    val wordCountDataStream = textDataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty).startNewChain()
      .map( (_, 1) )
      .keyBy(0)
      .process(new myProcessFunction)

    // 打印输出
    wordCountDataStream.print().setParallelism(1)

    // 执行任务
    env.execute("stream word count job")
  }

}

class myProcessFunction extends ProcessFunction[(String,Int),(String,Int)]{


  lazy val wordcount: ValueState[Int] = getRuntimeContext.getState(new
      ValueStateDescriptor[Int]("wordcount",classOf[Int]) )
  override def processElement(value: (String, Int), ctx: ProcessFunction[(String, Int), (String,Int)]#Context, out: Collector[(String,Int)]): Unit = {
    val word: String = value._1
    if(wordcount.value()==null){
      wordcount.update(1)
    }else{
      wordcount.update(wordcount.value()+1)
    }
    val subtask: Int = getRuntimeContext.getIndexOfThisSubtask
    println("subtask-"+subtask+":"+word)
    out.collect((word,wordcount.value()))

  }
}

class OperRichMapFunc extends RichFlatMapFunction with ListCheckpointed[String]{
  override def flatMap(value: Nothing, out: Collector[Nothing]): Unit = {

  }

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[String] = ???

  override def restoreState(state: util.List[String]): Unit = ???
}
