package com.sink

import java.util

import com.testAPI.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
  * @Classname ESSink
  * @Description TODO
  * @Date 2019/12/17 1:17
  * @Created by lz
  */
object ESSink {
  def main(args: Array[String]): Unit = {
    //导入隐式成员
    import org.apache.flink.api.scala._
    //  创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //  从文件中读取数据
    //    val inputPath = "in/SensorReading"
    //    val inputDS: DataStream[String] = env.readTextFile(inputPath)

    // 集合source
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))


    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("mydis", 9200))
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading]( httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext,
                             requestIndexer: RequestIndexer): Unit = {
          println("saving data: " + t)
          val json = new util.HashMap[String, String]()
          json.put("id", t.id)
          json.put("time", t.time.toString)
          json.put("temperature", t.temperature.toString)
          //构建请求动容对象
          val indexRequest =
            Requests.indexRequest().index("sensor").`type`("SensorReading").source(json)
          //理由参数列表中的请求器发送请求
          requestIndexer.add(indexRequest)
          println("saved successfully")
        }
      } )
    stream1.addSink( esSinkBuilder.build())
    env.execute()
  }

}
