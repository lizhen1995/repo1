package com.baseDemo

import com.atguigu.hotitems_analysis.ItemViewCount
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._


/**
  * @Classname TestSateManaged
  * @Description TODO
  * @Date 2020/3/10 16:54
  * @Created by lz
  */
object TestSateManaged {
  def main(args: Array[String]): Unit = {
    //  从外部命令中获取参数

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    env.enableCheckpointing(1000)//5秒一个checkpoint
    env.setStateBackend(new FsStateBackend("file:///E:\\myFlink\\out\\checkpoint1"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)//指定处理的时间特性
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(100)))//重启策略
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//确保一次语义
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)//退出不删除checkpoint
    // env.setStateBackend(new FsStateBackend("file:///E:/myFlink/out/checkpoints/074651996ccc748df56708beb2e7eaf5"))
    //  接收 socket 文本流
    val textDstream: DataStream[String] = env.socketTextStream("mydis", 8888)
    // flatMap 和 Map 需要引用的隐式转换
    import org.apache.flink.api.scala._
    val res: DataStream[String] = textDstream.flatMap(new MyFlatMap1)
    res.print()


    env.execute()
  }

}

class MyFlatMap1 extends RichFlatMapFunction[String,String] with CheckpointedFunction{

  private var offsetState: ListState[String] = _

  val list:ListBuffer[String]=new ListBuffer[String]

  override def flatMap(value: String, out: Collector[String]): Unit = {
    list.append(value)
    if(list.size>=3){
    out.collect(value)
      list.clear()
    }
  }

  override def open(parameters: Configuration): Unit = {
    //定义一个状态描述器
//    val stateDescriptor = new ListStateDescriptor[String]("offset-state", //TypeInformation.of(new TypeHint<Long>() {})
//      //Long.class
//      classOf[String]
//    )
//    offsetState = getRuntimeContext.getListState(stateDescriptor)
    println("open方法执行完毕")
  }

  @throws[Exception]
  override def initializeState(context: FunctionInitializationContext): Unit = { //定义一个状态描述器
    println("init方法执行")
    val stateDescriptor = new ListStateDescriptor[String]("offset-state", //TypeInformation.of(new TypeHint<Long>() {})
      //Long.class
     classOf[String])
    //初始化状态或获取历史状态(OperatorState)
    offsetState = context.getOperatorStateStore.getListState(stateDescriptor)
  }

  @throws[Exception]
  override def snapshotState(context: FunctionSnapshotContext): Unit = { //将历史值清除
    offsetState.clear()
    //根据最新的状态值
    //offsetState.update(Collections.singletonList(offset));
    offsetState.addAll(list)
  }
}
