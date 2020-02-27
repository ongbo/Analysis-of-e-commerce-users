package com.ongbo.NetWorkFlow_Analysis


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class UvCount(windowEnd: Long, uvCount: Long)
object UniqueView {
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
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())

    dataStream.print()
    env.execute()
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit ={
    //定义一个scala set， 用于保存所有的数据userID并去重
    //但是这个set是在内存，当前窗口非常大的话，放内存就很麻烦。
    var idSet = Set[Long]()
    //把当前窗口所有数据的ID收集到Set中，最后输出set的大小
    for(userBehavior <- input) {
      idSet += userBehavior.userId
    }
    out.collect(UvCount(window.getEnd, idSet.size))
  }
}
