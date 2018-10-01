package main.study.scala.com.tdj.study

import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}
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
    val schemaStr = "id,name,age,faceval"
    val fields = schemaStr.split(",").map(fieldName => StructField(fieldName,DataTypes.StringType,nullable = true))
//    val schema = StructType(fields)
   val schema = StructType(
     Seq(
       StructField("ID",DataTypes.IntegerType,nullable = true)
       ,StructField("Name",DataTypes.StringType,nullable = true)
       ,StructField("age",DataTypes.IntegerType,nullable = true)
       ,StructField("faceval",DataTypes.IntegerType,nullable = true)
     )
   )

        //  将RDD映射到rowRDD并创建
    val rowRDD = lineRDD.map(x => Row(x(0).toInt,x(1).toString,x(2).toInt,x(3).toInt))
    val personDF = sqlContext.createDataFrame(rowRDD,schema)

    //    创建DataFrame
//      val personDF = sqlContext.createDataFrame(personRDD)
//        注册表
    personDF.createOrReplaceTempView("t_person")

    //    查询
    val df:DataFrame = sqlContext.sql("select * from t_person")
    personDF.select("name","age","ID","faceval").filter("faceval>89").show()
    //    输出
//    df.show()
    sc.stop()
  }

}
