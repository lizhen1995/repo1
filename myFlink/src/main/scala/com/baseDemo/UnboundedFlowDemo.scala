package com.baseDemo

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Description：无界流演示（使用flink无界流处理实时监控netcat客户端和服务器交互的数据，进行实时的计算，将计算后的结果显示出来。）<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年02月24日
  *
  * @author 徐文波
  * @version : 1.0
  */
object UnboundedFlowDemo {
  def main(args: Array[String]): Unit = {

    //步骤：
    //①执行环境
   //val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
   // env.setParallelism(1)
    env.setParallelism(1)



    //②计算，输出
    //import org.apache.flink.api.scala._


    env.socketTextStream("mydis", 9999)
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map(x=>{
        println(x)
        new word(x,1)
      })
      .keyBy(_.value)
      .timeWindow( Time.seconds(10),Time.seconds(2) )

      .process(new mykedProcessFunc)
      .print()

    //③启动
    env.execute(this.getClass.getSimpleName)

  }
}

class mykedProcessFunc extends  ProcessWindowFunction[word,(String,Long),String,TimeWindow]{
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
  lazy val window: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("window-state", classOf[Long]))


  override def process(key: String, context: Context, elements: Iterable[word], out: Collector[(String, Long)]): Unit = {

    val start: Long = context.window.getStart
    if(window.value()==0){
      window.update(start)
    }
    if(window.value()!=start){
      countState.update(0)
    }
    for (elem <- elements) {
      val curCount = countState.value()
      println("当前状态为："+curCount)
      val newCount: Long = curCount+1

      countState.update(newCount)
      out.collect((key,newCount))
    }

  }
}







case class word(value:String,count:Int)
