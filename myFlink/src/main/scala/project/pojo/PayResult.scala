package project.pojo

/**
  * @Classname PayResult
  * @Description TODO
  * @Date 2019/12/21 14:24
  * @Created by lz
  */
case class PayResult(id: String,
                     serviceName:String,
                     bussinessRst:String,
                     chargefee:Double,
                     requestId:String ,
                     receiveNotifyTime :String,
                     provinceCode:Int)
