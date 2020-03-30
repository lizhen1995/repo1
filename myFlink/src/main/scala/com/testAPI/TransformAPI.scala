package com.testAPI

import org.apache.flink.api.common.functions.{FilterFunction, IterationRuntimeContext, RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
  * @Classname TransformAPI
  * @Description TODO
  * @Date 2019/12/16 17:40
  * @Created by lz
  */
object TransformAPI {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    val strDS: DataStream[String] = env.fromCollection(List("a b","c d"))
//    val res: DataStream[String] = strDS.flatMap(str=>str.split(" "))
//    res.print()
//    env.execute()

    val inputPath = "in/SensorReading"
     val inputDS: DataStream[String] = env.readTextFile(inputPath)
    val  sensorDS: DataStream[SensorReading] = inputDS.map(line => {
      val split: Array[String] = line.split(",")
      SensorReading(split(0).trim, split(1).trim.toLong, split(2).trim.toDouble)
    })

   // val res: DataStream[SensorReading] = sensorDS.keyBy(_.id).sum("temperature")
//    val res: DataStream[SensorReading] = sensorDS.keyBy(_.id).reduce((x,y)=>SensorReading(y.id,y.time+10,x.temperature+1))
//    res.print().setParallelism(1)

    //分流
    val splitDS: SplitStream[SensorReading] = sensorDS.split(s => {
      if (s.temperature > 30) Seq("high") else Seq("low")
    })
    val highDS: DataStream[SensorReading] = splitDS.select("high")
    val lowDS: DataStream[SensorReading] = splitDS.select("low")
    val allDS: DataStream[SensorReading] = splitDS.select("high","low")
    highDS.print("high").setParallelism(4)
    lowDS.print("low").setParallelism(4)
    allDS.print("all").setParallelism(4)
    println("===============================================")
    //合并
    val warningDS: DataStream[(String, Double)] = highDS.map(s=>(s.id,s.temperature))
    val conectDS: ConnectedStreams[(String, Double), SensorReading] = warningDS.connect(lowDS)
    val res: DataStream[Product] = conectDS.map(t=>{(t._1,t._2,"warning")},s=>{(s.id,"healthy")})
    res.print("res")

    //union
    val unionDS: DataStream[SensorReading] = highDS.union(lowDS)
    unionDS.print("unionDS")


    val myFilterDS: DataStream[SensorReading] = sensorDS.filter(new myFilter())
    env.execute()

    val flatDS: DataStream[(Int, String)] = sensorDS.flatMap(new MyFlatMap())
    flatDS.print("flatMap")
    env.execute()
    val flatDS1: DataStream[(Int, String)] = sensorDS.flatMap(new MyFlatMap())
    flatDS1.print("flatMap1")
    env.execute()

  }

}

class myFilter extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}

class MyFlatMap extends RichFlatMapFunction[SensorReading,(Int, String)] {
  var subTaskIndex = 0
  override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

  override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

  override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

  override def open(configuration: Configuration): Unit = {
    println("open:开启")
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
  }
  override def flatMap(in: SensorReading, out: Collector[(Int, String)]): Unit = {
   out.collect(subTaskIndex,in.id)
  }
  override def close(): Unit = {
    println("close:关闭")
  }
}

