package main.study.scala.com.tdj.study

import java.sql.{Connection, DriverManager}

import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.concurrent.duration.Duration

/**
  * 实时按批次累计的功能,实现WordCount
  */
object StreamingWC {
  def main(args: Array[String]): Unit = {
//    myLog.setLogLeavel(Level.ERROR)
    val conf = new SparkConf().setAppName("StreamingWC").setMaster("local[2]")
    conf.set("spark.testing.memory", "536870912")
    conf.set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)
//    sc.setLogLevel(Level.ERROR)
    val ssc = new StreamingContext(conf,Seconds(2))
    ssc.remember(Durations.seconds(2))
    //拉取socket的信息
    val dStream = ssc.socketTextStream("localhost",10086)
//   Window 环境下用命令 nc -l -p 10086
//   Linux 下用 nc -lk 10086
    //使用updateStateByKey这个算子必须设置setCheckpointDir
    sc.setCheckpointDir("C:\\data\\checkpoint")
    ssc.checkpoint("C:\\data\\checkpoint")

//    计算wordcount累计
    val res = dStream.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc,new HashPartitioner(sc.defaultParallelism),false)
    //计算wordcount 每个批次
//        val res = dStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
//    结果打印到控制台
    res.print()

//    结果写到Mysql
    res.foreachRDD({
      rdd => rdd.foreachPartition({
        it => {
          val conn:Connection = DriverManager.getConnection("jdbc:mysql://192.168.8.212:3306/lianxi","root","Root@1234")
          it.foreach({
              wc =>
                 val pst = conn.prepareStatement("select * from lianxi.t_wc where wordval = ?")
                 pst.setString(1,wc._1)
                 val rs = pst.executeQuery()
                 var flag = true

              while(rs.next()){
                 flag = false
                 val preCount = rs.getInt("wordcnt")
                 val pst1 = conn.prepareStatement("update t_wc set wordcnt = ? where wordval = ?")
                 pst1.setInt(1, wc._2 + preCount)
                 pst1.setString(2, wc._1)
                 pst1.executeUpdate()
                 pst1.close()

              }
                rs.close()
                pst.close()
              if(flag){
                val pst1 = conn.prepareStatement("insert into t_wc values(?,?)")
                pst1.setString(1, wc._1)
                pst1.setInt(2, wc._2)
                pst1.execute()
                pst1.close()

              }

              try{}
              conn.close()
          })
        }
      })
    })
//
    ssc.start()
    ssc.awaitTermination()
  }
  val updateFunc = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }
}
