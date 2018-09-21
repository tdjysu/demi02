package main.study.scala.com.tdj.study

import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo01 {
  def main(args: Array[String]): Unit = {
    //设置本机Spark配置
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[1]")
    conf.set("spark.testing.memory", "471859200")
    //创建Spark上下
    val sc = new SparkContext(conf)
    //从文件中获取数据
    val input = sc.textFile("C:\\tmp\\pv")
    //分析并排序输出统计结果
    input.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y).sortBy(_._2,false).foreach(println _)

  }
}
