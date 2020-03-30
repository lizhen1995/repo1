package project.pojo.service

import java.util.UUID

import scala.collection.immutable.List

/**
  * @Classname Test
  * @Description TODO
  * @Date 2019/12/21 14:36
  * @Created by lz
  */
object Test {
  def main(args: Array[String]): Unit = {

    var list1 = List(1,2,4)
    val list2 = List(1,2,4)
     list1 = list1 :+ 3
    println(list1)

  }
}
