package project.pojo.service

import java.{lang, util}
import java.util.UUID

import com.alibaba.fastjson.JSON
import com.sink.RedisExampleMapper
import com.testAPI.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import project.pojo.PayResult

/**
  * @Classname OneRequest
  * @Description TODO
  * @Date 2019/12/21 14:29
  * @Created by lz
  */
object OneRequest {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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

    val splitDS: SplitStream[PayResult] = payResultDS.split(pay => {
      if (pay.bussinessRst.equals("0000")) Seq("success") else Seq("failed")
    })


    val successDS: DataStream[(PayResult,Int)] = splitDS.select("success").map((_,1))
    val failedDS: DataStream[PayResult] = splitDS.select("failed")
    val allDS: DataStream[(PayResult,Int)] = splitDS.select("success","failed").map((_,1))
    val countDS: DataStream[Int] = allDS.keyBy(_._2).process(new myCountProcess())

     val sucDS: DataStream[(String, Int, Double)] = successDS.keyBy(_._2).process(new myOrderCountProcess())


      val sucRes: DataStream[String] = sucDS.map(t=>{"time:"+t._1+" sucOrderNum:"+t._2+" allMoney:"+t._3})
    val res: DataStream[String] = sucRes.union(countDS.map("  allOrderNum:"+_.toString))
    res.print("res")
    val conf = new FlinkJedisPoolConfig.Builder().setHost("mydis").setPort(6379)build()
    res.addSink(new RedisSink[String](conf, new RedisExampleMapper))
    env.execute()


  }



class myCountProcess  extends ProcessFunction[(PayResult,Int), Int]{
  private var lastTempState: ValueState[Int] = _
  override def open(parameters: Configuration): Unit = {
    // 初始化的时候声明state变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("countTemp", classOf[Int]))

  }
  override def processElement(i: (PayResult,Int), context: ProcessFunction[(PayResult,Int), Int]#Context, collector: Collector[Int]): Unit = {

      lastTempState.update(lastTempState.value()+1)
      collector.collect(lastTempState.value())
  }
}

class myOrderCountProcess extends ProcessFunction[(PayResult,Int), (String,Int,Double)]{
  private var lastTempCountState: ValueState[Int] = _
  private var lastTempMoneyState: ValueState[Double] = _
  override def open(parameters: Configuration): Unit = {
    // 初始化的时候声明state变量
    lastTempCountState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("countTemp", classOf[Int]))
    lastTempMoneyState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("moneyTemp", classOf[Double]))
  }
  override def processElement(i: (PayResult, Int), context: ProcessFunction[(PayResult, Int), (String, Int, Double)]#Context, collector: Collector[(String, Int, Double)]): Unit = {
    lastTempCountState.update(lastTempCountState.value()+1)
    lastTempMoneyState.update(lastTempMoneyState.value()+i._1.chargefee)
    collector.collect((i._1.receiveNotifyTime,lastTempCountState.value(),lastTempMoneyState.value()))
  }
}
  class RedisExampleMapper extends  RedisMapper[String]{
    //定义保存数据到redis的命令
    override def getCommandDescription: RedisCommandDescription = {
      //把传感器的数据保存成HSET  key  filed value
      new RedisCommandDescription(RedisCommand.HSET,"One_result")
    }

    override def getKeyFromData(t: String): String = "result"

    override def getValueFromData(t: String): String = t
  }


}

