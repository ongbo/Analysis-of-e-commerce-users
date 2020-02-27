package com.ongbo.NetWorkFlow_Analysis

import com.ongbo.NetWorkFlow_Analysis.UniqueView.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvWithBloom {
 def main(args: Array[String]): Unit = {
   val env = StreamExecutionEnvironment.getExecutionEnvironment
   env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
   env.setParallelism(1)

   //用相对路径定义数据集
   val resource = getClass.getResource("/UserBehavior.csv")
   val dataStream = env.readTextFile("/Users/ongbo/Maven/bin/UserBehaviorAnalysis/NetWorkFlowAnalysis/src/main/resources/UserBehavior.csv")
     .map(data =>{
       val dataArray = data.split(",")
       UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
     })
     .assignAscendingTimestamps(_.timestamp * 1000L)
     .filter(_.behavior.equals("pv"))
     .map( data => ("dummyKey",data.userId))
     .keyBy(_._1)
     .timeWindow(Time.hours(1))
     .trigger(new MyTrigger())
     .process(new UvCountWithBloom())

   dataStream.print()
   env.execute()
 }
}


//自定义窗口触发器
class MyTrigger() extends Trigger[(String,Long),TimeWindow]{
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //每来一条数据就直接触发窗口操作，并清空所有状态
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}
class UvCountWithBloom() extends ProcessWindowFunction[(String,Long),UvCount,String, TimeWindow] {
  // 定义Redis连接
  lazy val jedis = new Jedis("114.116.219.97",5000)
  //29位，也就是64M
  lazy val bloom = new Bloom(1 << 29)
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //位图的存储方式 , key是windowwen，value是位图
    val storeKey = context.window.getEnd.toString
    var count = 0L
    //把每个窗口的count值，也存入Redis表里，存放内容位（windowEnd，uccount），所以要先从Redis中读取


    if(jedis.hget("count",storeKey) != null){
//      System.out.println(v)
      count = jedis.hget("count",storeKey).toLong
    }
    //用布隆过滤器判断当前用户是否已经存在
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    //定义一个标志位，判断Redis位图中有没有这一位
    val isExist = jedis.getbit(storeKey, offset)
    if(!isExist){
      //如果不存在位图对应位置变成1，count+1
      jedis.setbit(storeKey,offset,true)
      jedis.hset("count",storeKey,(count+1).toString)
      out.collect(UvCount(storeKey.toLong,count+1))
    }else{
      out.collect(UvCount(storeKey.toLong,count))
    }

  }
}

class Bloom(size: Long) extends Serializable{
  //位图大小
  private val cap = if(size>0) size else 1 << 27

  //定义Hash函数
  def hash(value: String, seed: Int) : Long = {
    var result:Long = 0L
    for(i <- 0 until value.length){
      result = result * seed + value.charAt(i)
    }

    result & (cap-1)
  }
}
