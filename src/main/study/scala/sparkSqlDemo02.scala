import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._

//case class Person(id:Int,name:String,age:Int,faceValue:Int)
case class Person(id:Int,name:String,age:Int,faceValue:Int)

object sparkSqlDemo01 {



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparksql").setMaster("local[2]")
    conf.set("spark.testing.memory", "536870912")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
//    val sparkSession = SparkSession.builder().appName("sparksql").config("spark.some.config.option","some_value").getOrCreate()
//    sparkSession.sparkContext.textFile("C:\\data\\person.txt").map(_.split(",")).map(x => Person(x(0).toInt,x(1),x(2).toInt,x(3).toInt)).toDF()
    val dataRdd = sc.textFile("C:\\data\\person.txt").map(_.split(","))
    val personRDD = dataRdd.map(x => Person(x(0).toInt,x(1),x(2).toInt,x(3).toInt))

    val structFields = new util.ArrayList[StructField]()
    structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true))
    structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true))
    structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true))
    structFields.add(DataTypes.createStructField("faceValue",DataTypes.IntegerType,true))

    val st = DataTypes.createStructType(structFields)

    val personDF = sqlContext.createDataFrame(personRDD)
//    personDF.show()
    personDF.registerTempTable("tempPerson")
//   SQL查询数据写法
//    sqlContext.sql("select * from tempPerson where id >=3").show()
//    DSL查询数据写法
//    personDF.select(personDF.col("name")).show()
//    personDF.select(col(""))

  }

}


