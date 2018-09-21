package main.study.scala.com.tdj.study

import org.apache.spark.{SparkConf, SparkContext}

object CustomSort {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CustomSort").setMaster("local[2]")
    conf.set("spark.testing.memory", "536870912")
    val sc = new SparkContext(conf)

    val girlInfo = sc.parallelize(Array(("tingitng",80,25),("ningning",90,26),("mimi",90,27)))

    val res = girlInfo.sortBy(_._2,false)
    println(res.collect().toBuffer)
    sc.stop()
  }
}
