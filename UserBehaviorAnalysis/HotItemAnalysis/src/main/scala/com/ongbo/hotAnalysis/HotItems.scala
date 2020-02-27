package com.ongbo.hotAnalysis

import java.sql.Timestamp

import com.ongbo.NetWorkFlow_Analysis
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/*
*定义输入数据的样例类
 */
case class UserBehavior(userId: Long, itemId: Long, cateGoryId: Int,behavior: String, timestamp: Long)
//定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    //1:创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置为事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //2:读取数据
    val dataStream = env.readTextFile("/Users/ongbo/Maven/bin/UserBehaviorAnalysis/HotItemAnalysis/src/main/resources/UserBehavior.csv")
      .map(data =>{
        val dataArray = data.split(",")
        NetWorkFlow_Analysis.UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //3:transform处理数据
    val processStream = dataStream
      //筛选出埋点pv数据
      .filter(_.behavior.equals("pv"))
      //先对itemID进行分组
      .keyBy(_.itemId)
      //然后设置timeWindow，size为1小时，步长为5分钟的滑动窗口
      .timeWindow(Time.hours(1), Time.minutes(5))
      //窗口聚合
      .aggregate(new CountAgg(), new WindowResult())
      //按照窗口分组
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(10))


    //sink：输出数据
    processStream.print("processStream::")
//    dataStream.print()
    //执行
    env.execute("hot Items Job")



  }
}

/*自定义预聚合函数*/
class CountAgg() extends AggregateFunction[NetWorkFlow_Analysis.UserBehavior, Long, Long]{
  //累加器初始值
  override def createAccumulator(): Long = 0
  //每来一次就加一
  override def add(in: NetWorkFlow_Analysis.UserBehavior, acc: Long): Long = acc+1
  //
  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义窗口函数，输出ItemViewCount
class WindowResult() extends WindowFunction[Long,ItemViewCount, Long, TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit =  {
    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}

//自定义处理函数
class TopNHotItems(topsize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))

  }
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //把每条数据存入状态列表
    itemState.add(value)
    //注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }
  //定时器触发时，对所有的数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: _root_.org.apache.flink.streaming.api.functions.KeyedProcessFunction[Long, _root_.com.ongbo.hotAnalysis.ItemViewCount, _root_.scala.Predef.String]#OnTimerContext, out: _root_.org.apache.flink.util.Collector[_root_.scala.Predef.String]): Unit = {
    //将所有state中的数据取出，放到一个list Buffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for(item <- itemState.get()){
      allItems += item
    }

    //按照点计量count大小排序,sortBy默认是升序,并且取前三个
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topsize)

    //清空状态
    itemState.clear()

    //格式化输出排名结果
    val result : StringBuilder = new StringBuilder
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    //输出每一个商品信息
    for(i<- sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No").append(i+1).append(":")
        .append("  商品ID：").append(currentItem.itemId)
        .append("  浏览量：").append(currentItem.count).append("\n")
    }
    result.append("============================\n")
    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}




/*自定义预聚合函数计算平均数*/
class AverageAgg() extends AggregateFunction[NetWorkFlow_Analysis.UserBehavior, (Long,Int), Double]{
  override def createAccumulator(): (Long, Int) = (0L,0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1+in.timestamp, acc._2+1)

  override def getResult(acc: (Long, Int)): Double = acc._1 /acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1+acc1._1, acc._2+acc1._2)
}
