package com.testAPI

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  *
  *
  */                        // id       行为               下载渠道
//输入 的样例类
case  class   Userberveriover(userid:String,beavior:String,channel :String,timeStamp:Long)

//输出的样例类
case class   MarktingViewCount(windowStart:String,windowEnd:String,channel:String,beavior:String,count:Long)

object AppMaketBychannel {
  def main(args: Array[String]): Unit = {
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val datastream =env.addSource(new SimulateEventSource())
      .assignAscendingTimestamps(_.timeStamp)  //数据是升序来了每乱序，不需要延迟处理
      .filter(_.beavior!= "UNInstall")
      .map(data=>{
        ((data.channel,data.beavior),1L)
      }).keyBy(_._1)   //以渠道和行为类型作为key分组
      .timeWindow(Time.hours(1),Time.seconds(10))
      .process(new  MaketingCountByChannel() )


    datastream.print()
    env.execute("app maket job ")

  }

}
//自定义处理函数
class  MaketingCountByChannel extends  ProcessWindowFunction[((String,String),Long),MarktingViewCount,(String,String),TimeWindow]{
  override def process(key: (String,String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarktingViewCount]): Unit = {
    //这个是窗口闭合的时候关闭  有了该窗口的所有的数据
    //可以计数和去重   数据量大了不行
    val startTS =new Timestamp(context.window.getStart).toString   //long类型转下
    val endTS =new Timestamp(context.window.getEnd).toString
    val channel =key._1
    val behavior=key._2
    val count =elements.size
    out.collect(MarktingViewCount(startTS,endTS,channel,behavior,count))
  }
}

//自定义数据源
class   SimulateEventSource()  extends  RichSourceFunction[Userberveriover]{
  //事先定义运行的标志位
  var ruinng = true
  //定义行为的集合
  val beaviorTypes :Seq[String]=Seq("CLICK","Install","UNInstall","Download")
  //定义渠道的集合
  val channelSets:Seq[String]=Seq("weibo","huawei","appstore","wechat")
  //随机数发生器
  val rand :Random = new Random()
  override def cancel(): Unit = {
    ruinng =false
  }

  override def run(ctx: SourceFunction.SourceContext[Userberveriover]): Unit = {
    //定义一个生成数据的上限
    val maxData=Long.MaxValue
    var Count =0L  //计数
    //随机生成所有的数据
    while(ruinng && Count < maxData){
      val usertid=UUID.randomUUID().toString
      val behavior=beaviorTypes(rand.nextInt(beaviorTypes.size))
      val channel =channelSets(rand.nextInt(channelSets.size))
      val ts =System.currentTimeMillis()
      ctx.collect(Userberveriover(usertid,behavior,channel,ts))
      Count +=1
      TimeUnit.MICROSECONDS.sleep(10L)   //休眠10毫秒
    }
  }
}