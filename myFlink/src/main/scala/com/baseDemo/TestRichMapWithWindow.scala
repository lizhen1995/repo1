package com.baseDemo

import java.lang

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Classname TestRichMapWithWindow
  * @Description TODO
  * @Date 2020/1/30 20:58
  * @Created by lz
  */
object TestRichMapWithWindow {
  def main(args: Array[String]): Unit = {
    //  从外部命令中获取参数

    //  创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    //  接收 socket 文本流
    val textDstream: DataStream[String] = env.socketTextStream("mydis", 7777)
    //val dataStream: DataStream[String] = textDstream.map(new myRichMapFunction1)
    // flatMap 和 Map 需要引用的隐式转换
    import org.apache.flink.api.scala._
     //val windowData: WindowedStream[Word, String, TimeWindow] = textDstream.map(new Word(_)).keyBy(_.value).timeWindow(Time.seconds(6), Time.seconds(1))
     val keyed: KeyedStream[Word, String] = textDstream.flatMap(new myFlatMapFunction).map(new Word(_)).keyBy(_.value)
    //val windowData: AllWindowedStream[String, TimeWindow] = textDstream.timeWindowAll(Time.seconds(2))
    //val dataStream: DataStream[String] = keyed.process(new MyProcess()).setParallelism(2)
    //val dataStream: DataStream[Long] = windowData.aggregate(new CountAgg).setParallelism(1)
//    val dataStream: DataStream[(String, Int)] =
//      windowData.flatMap(_.split("\\s")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)
//    val dataStream: DataStream[String] = keyed.map(new myRichMapFunction())
//    dataStream.print().setParallelism(4)
    keyed.flatMap(new myRichMapFunction3)
   // keyed.process(new myKeyedProcessFunction).setParallelism(4).print()

    //  启动 executor ，执行任务
    env.execute("Socket stream word count another")
  }


}


case class Word(value: String)

class MyProcess1 extends ProcessWindowFunction[Word,String,String,TimeWindow]{


  override def open(parameters: Configuration): Unit = {
    println("open 方法执行")
  }

  override def process(key: String, context: Context, elements: Iterable[Word], out: Collector[String]): Unit = {
    println("process 方法执行")
  }
}

class MyProcess extends KeyedProcessFunction[String,Word,String]{


  override def open(parameters: Configuration): Unit = {
    println("open 方法执行")
  }

  override def processElement(i: Word, context: KeyedProcessFunction[String, Word, String]#Context, collector: Collector[String]): Unit = {
    println("process 方法执行")
  }
}

class CountAgg() extends AggregateFunction[String, Long, Long]{
  override def add(value: String, accumulator: Long): Long = accumulator + 1L
  override def createAccumulator(): Long = 0L
  override def getResult(accumulator: Long): Long = accumulator
  override def merge(a: Long, b: Long): Long = a + b
}

class myRichMapFunction extends RichMapFunction[Word,String]{

  var count:Int=0
  override def open(parameters: Configuration): Unit = {

    println("open 方法执行")
  }

  override def map(value: Word): String = {
    println("map 方法执行")
    count+=1
     value.value+count
  }
}

class myRichMapFunction1 extends RichMapFunction[String,String] {
  override def open(parameters: Configuration): Unit = {
    println("open 方法执行")
  }

  override def map(value: String): String = {
    println("map 方法执行")
    value
  }
}

class myKeyedProcessFunction extends KeyedProcessFunction[String ,Word,String]{

  var count:Int=0
  override def open(parameters: Configuration): Unit = {
    println("open 方法执行")
  }

  override def processElement(value: Word, ctx: KeyedProcessFunction[ String,Word, String]#Context, out: Collector[String]): Unit = {
    println("process 执行")
    count+=1
    println(count)
  }
}

class myRichMapFunction3 extends RichFlatMapFunction[Word,String]{
  override def flatMap(value: Word, out: Collector[String]): Unit = {}
}