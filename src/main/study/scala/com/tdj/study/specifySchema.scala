package main.study.scala.com.tdj.study

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object specifySchema {

  def main(args: Array[String]): Unit = {
    //模板代码
    val conf = new SparkConf().setAppName("InferrSchema").setMaster("local[1]")
    conf.set("spark.testing.memory", "536870912")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //    获取数据
    val lineRDD = sc.textFile("C:\\data\\person.txt").map(_.split(","))

    //    通过StructType指定每个字段的schema


        //  将RDD映射到rowRDD并创建
    val rowRDD = lineRDD.map(x => Row(x(0).toInt,x(1),x(2),x(3).toInt))
//    val personDF = sqlContext.createDataFrame(rowRDD,schema)



    //    创建DataFrame
    //  val personDF = sqlContext.createDataFrame(personRDD)
    //    注册表
//    personDF.registerTempTable("t_person")

    //    查询
    val df:DataFrame = sqlContext.sql("select * from t_person")

    //    输出
    df.show()
    sc.stop()
  }

}
