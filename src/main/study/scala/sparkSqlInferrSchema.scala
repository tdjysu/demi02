import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object sparkSqlInferrSchema {
  def main(args: Array[String]): Unit = {
//模板代码
    val conf = new SparkConf().setAppName("InferrSchema").setMaster("local[1]")
    conf.set("spark.testing.memory", "536870912")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

//    获取数据
    val lineRDD = sc.textFile("D:\\log_data\\person.txt").map(_.split(","))

//    将RDD和case class进行关联
    val personRDD = lineRDD.map(x => Person2(x(0).toInt,x(1),x(2).toInt,x(3).toInt))



//    创建DataFrame
      import  sqlContext.implicits._
      val personDF = sqlContext.createDataFrame(personRDD)
//    注册表
       personDF.registerTempTable("t_person")

    //    查询
    val df:DataFrame = sqlContext.sql("select * from t_person")

//    输出
      df.show()
    sc.stop()

  }
}

case class Person2(id:Int,name:String,age:Int,faceValue:Int)