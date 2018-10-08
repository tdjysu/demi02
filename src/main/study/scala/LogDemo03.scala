package scala

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 缓存机制
  * 自定义一个分区器
  * 按照每种学科数据放到不同的分区器里
  */
object LogDemo03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("logDemo").setMaster("local[1]")
    conf.set("spark.testing.memory", "536870912")
//    conf.set("spark.driver.memory", "536870912")
    val sc = new SparkContext(conf)
    val logRdd = sc.textFile("C:\\data\\sdk_log")
//  根据分割符切分字段,并生成一个元组,并缓存起来
    val urlOneLine = logRdd.map(line => {
      val fields = line.split("\n")
      val url = fields(0).filter( x => x != "")
      (url,1)
    }).map( ele => {
      //  获取data对象
      val dataObj = JSON.parseObject(JSON.parseObject(ele._1.toString).get("data").toString)
      val value = dataObj.get("properties")
      val event_val = dataObj.get("event_id")
      //println("dataproperties-->" + dataObj.get("properties"))
      val paraObj:fastjson.JSONObject = JSON.parseObject(dataObj.get("properties").toString)
//      println("deviceID-->" + paraObj.get("deviceId") + "  event_id-->" +event_val )
      (event_val,paraObj.get("deviceId"))
     }
    ).groupByKey().map(line => (line._1,line._2.toList.distinct.size)).cache()

    //调用Spark自带的分区器，此时会发生哈希碰撞，需要自定义分区器
//   val res:RDD[(String,(String,Int))] = catchedUrlInfo.partitionBy(new HashPartitioner(3))
//  得到所有学科
/*    val projects:Array[String] = urlOneLine.keys.distinct.collect
//    调用自定义分区器并得到分区号
    val partitioner:ProjectPartitioner = new ProjectPartitioner(projects)
//  进行分区
    val partitionRDD:RDD[(String,(String,Int))] = catchedUrlInfo.partitionBy(partitioner)
//   对每个分区的数据进行排序并获取top3
    val res:RDD[(String,(String,Int))] = partitionRDD.mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(3).iterator
    })*/

    urlOneLine.saveAsTextFile("C:\\data\\out")

//    根据Url中具体信息进行聚合
 //   val res:RDD[(String,List[(String,String,Int)])] = catchedUrlInfo.groupBy(_._1).mapValues(_.toList.sortBy(_._3).reverse.take(3))

  //  println(res.collect.toBuffer)
    sc.stop()
  }

  class ProjectPartitioner(projects:Array[String]) extends Partitioner{
//    用来存放学科和分区号
    private val projectHashMap = new mutable.HashMap[String,Int]
// 计数器,用于指定分区号
    var n = 0

    for (pro <- projects){
      projectHashMap += (pro -> n)
//      projectHashMap.put(pro,n)
      n += 1
    }
//    得到分区数
    override def numPartitions: Int = projects.length
//   得到分区号
    override def getPartition(key: Any): Int = {
      projectHashMap.getOrElse(key.toString,0)
    }
  }

}
