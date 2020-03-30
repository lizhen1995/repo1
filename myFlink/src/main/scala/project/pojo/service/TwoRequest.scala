package project.pojo.service

import java.util.UUID

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import project.pojo.PayResult

/**
  * @Classname TwoRequest
  * @Description TODO
  * @Date 2019/12/21 17:50
  * @Created by lz
  */
object TwoRequest {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(10000)//10秒一个checkpoint
    env.setStateBackend(new FsStateBackend("file:///E:/myFlink/out/checkpoint"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//指定处理的时间特性
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,org.apache.flink.api.common.time.Time.seconds(50)))//重启策略RestartStrategies.fixedDelayRestart(3,Time.seconds(100))，会在100秒内重试3次，失失败事务中到开始到错误数据段的数据
    //env.setRestartStrategy(RestartStrategies.fallBackRestart())//重启策略 RestartStrategies.fallBackRestart()失败后立即重启，丢失失败事务中到开始到错误数据段的数据
    //env.setRestartStrategy(RestartStrategies.noRestart())//重启策略 RestartStrategies.fallBackRestart()失败后立即重启，丢失失败事务中到开始到错误数据段的数据
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//确保一次语义
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)//退出不删除checkpoint
    // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
    env.getCheckpointConfig.setFailOnCheckpointingErrors(true)
    val textDstream: DataStream[String] = env.socketTextStream("mydis", 9888)
    val payResultDS: DataStream[PayResult] = textDstream.map(line => {
      val serviceName: String = JSON.parseObject(line).getString("serviceName")
      val chargefee: Double = JSON.parseObject(line).getDouble("chargefee")
      val requestId: String = JSON.parseObject(line).getString("requestId")
      val receiveNotifyTime: String = JSON.parseObject(line).getString("receiveNotifyTime")
      val provinceCode: Int = JSON.parseObject(line).getIntValue("provinceCode")
      val bussinessRst: String = JSON.parseObject(line).getString("bussinessRst")


      //UUID.fromString()
      val id: String = UUID.randomUUID().toString
      new PayResult(id, serviceName, bussinessRst,chargefee, requestId, receiveNotifyTime, provinceCode)
    })
    payResultDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PayResult](Time.seconds(1)) {
      override def extractTimestamp(element: PayResult): Long = element.receiveNotifyTime.toInt * 1000
    })
  }

}
