package main.study.scala.com.tdj.study

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LogDemo01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("logDemo").setMaster("local")
    conf.set("spark.testing.memory", "471859200")
    val sc = new SparkContext(conf)

    val logRdd = sc.textFile("C:\\data\\urldata.csv")
//  根据分割符切分字段
    val urlOneLine = logRdd.map(line => {
      val fields = line.split(",")
      val url = fields(1)
      (url,1)
    })
//   把相同的url内容进行聚苑合
    val sumUrl = urlOneLine.reduceByKey(_+_)
//    获取Url中具体信息
    val urlInfoRdd:RDD[(String,String,Int)] = sumUrl.map(x =>{
      val urlinfo = x._1
      val cnt = x._2
      val urlfield = new URL(urlinfo).getHost
      (urlfield,urlinfo,cnt)
    })
//    根据Url中具体信息进行聚合
    val res:RDD[(String,List[(String,String,Int)])] = urlInfoRdd.groupBy(_._1).mapValues(_.toList.sortBy(_._3).reverse.take(3))

    println(res.collect.toBuffer)
  }
}
