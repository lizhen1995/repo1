package com.baseDemo

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}


/**
  * @Classname WordCount
  * @Description TODO
  * @Date 2019/12/16 9:37
  * @Created by lz
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //导入隐式成员
    import org.apache.flink.api.scala._
    //  创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //  从文件中读取数据
    val inputPath = "in/wordCount.txt"
      val inputDS: DataSet[String] = env.readTextFile(inputPath)

    val wordCountDS: AggregateDataSet[(String, Int)] = inputDS
      .flatMap(_.split("\\s+")).setParallelism(3)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    //  打印输出
    wordCountDS.print()

    env.execute("WordCount")
  }

}
