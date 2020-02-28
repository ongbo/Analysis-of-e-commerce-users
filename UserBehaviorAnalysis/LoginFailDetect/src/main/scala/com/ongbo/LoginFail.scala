package com.ongbo

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
//输入的登陆时间样例类
case class LoginEvent(userId: Long, ip:String, eventType: String, eventTime: Long)
//异常信息报警样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //
    val resource = getClass.getResource("/LoginLog.csv")
    val loginFail = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong,dataArray(1).trim,dataArray(2).trim,dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })
    val warningStream = loginFail.keyBy(_.userId)
      .process(new LoginWarning(5))


    warningStream.print()
    env.execute("login fail detect job")

  }
}

class LoginWarning(i: Int) extends KeyedProcessFunction[Long,LoginEvent,Warning] {

  //定义状态
  lazy val longFailState : ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state",classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {

//    val loginFailList = longFailState.get()//拿到当前状态
//    //判断类型是不是fail，只添加fail的状态
//    if(value.eventType == "fail"){
//      if( ! loginFailList.iterator().hasNext){
//        ctx.timerService().registerEventTimeTimer( value.eventTime * 1000 + 2000L)
//
//      }
//      longFailState.add(value)
//
//    }else {
//      //如果是成功，就直接清空状态
//      longFailState.clear()
//    }

    if(value.eventType == "fail"){
      //如果是失败，判断之前是否有登陆失败事件
      val iter = longFailState.get().iterator()
      if(iter.hasNext){
        //如果已经有登陆事件，那么就比较事件事件是不是在i秒内
        val firstFail = iter.next()
        if(value.eventTime < firstFail.eventTime +2){
          //如果在两秒钟之内,但是可能有乱序问题
          out.collect(Warning(value.userId,firstFail.eventTime, value.eventTime,"login fail in 2 seconds"))

        }
        longFailState.clear()
        longFailState.add(value)
      }else{
        //如果是第一次登陆失败，直接添加进去
        longFailState.add(value)
      }
    }else {
      longFailState.clear()
    }
  }

//  override def onTimer(timestamp: Long, ctx: _root_.org.apache.flink.streaming.api.functions.KeyedProcessFunction[Long, _root_.com.ongbo.LoginEvent, _root_.com.ongbo.Warning]#OnTimerContext, out: _root_.org.apache.flink.util.Collector[_root_.com.ongbo.Warning]): Unit = {
//    //触发定时器的时候，根据状态里面的失败个数有多少个决定输出报警
//    val llLoginails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
//    val iter = longFailState.get().iterator()
//    while(iter.hasNext){
//      llLoginails += iter.next()
//    }
//
//    //判断个数
//    if(llLoginails.length >= i){
//      out.collect(Warning(llLoginails.head.userId,llLoginails.head.eventTime,llLoginails.last.eventTime,"login fail in 2 seconds for"))
//    }
//  }
}















