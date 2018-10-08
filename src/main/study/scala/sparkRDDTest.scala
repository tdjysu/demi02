package scala

import org.apache.spark.{SparkConf, SparkContext}

object sparkRDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparktest").setMaster("local")
    conf.set("spark.testing.memory", "471859200")
    val sc = new SparkContext(conf)
//    通过并行化生成RDD
    val rdd1 = sc.parallelize(List(2,3,5,6,8,5,23,45))
    val rs1 = rdd1.map(_ * 2).sortBy( _.toInt,false)
//过滤大于10的元素
    val rs2 = rs1.filter( _ > 10)
//    将元素以数组的方式打印出来
//    println(rs2.collect().toBuffer)
    val rdd2 = sc.parallelize(Array("a b c","d e f","h i j"))
    val rs3 = rdd2.flatMap(_.split(" "))
//    println(rs3.collect.toBuffer)

//    来个复杂的
     val rdd3 = sc.parallelize(List(List("a b c","a b b"),List("e f g","a f g"),List("h i j","a a b")))
    val rs4 = rdd3.flatMap(_.flatMap(_.split(" ")))
//    println(rs4.collect.toBuffer)

    val rdd4 = sc.parallelize(List(1,2,3,4))
    val rdd5 = sc.parallelize(List(5,6,5,3))
//    RDD取并集
    val rs5 = rdd4 union rdd5
//    println(rs5.collect.toBuffer)
//  RDD取交集
//    println(rdd4.intersection(rdd5).collect.toBuffer)
//    去重
//    println(rs5.distinct.collect.toBuffer)
    val rdd6 = sc.parallelize(List(("tom",1),("jerry",3),("kitty",2)))
    val rdd7 = sc.parallelize(List(("jerry",2),("tom",1),("shark",2)))
//    inner join
//    println((rdd6.join(rdd7).collect.toBuffer))
//    左连接 右连接
    val rs6 = rdd6.leftOuterJoin(rdd7)
    val rs7 = rdd6.rightOuterJoin(rdd7)
//    按key分组
//    println((rdd6 union rdd7).groupByKey.collect.toBuffer)

//   分别用groupByKey和reduceByKey实现单词计数，注意groupByKey和reduceByKey的区别

//   groupByKey
//    println((rdd6 union rdd7).groupByKey().mapValues(_.sum).collect.toBuffer)
//  reduceByKey
//    println((rdd6 union rdd7).reduceByKey(_ + _).collect.toBuffer)

    val rdd8 = sc.parallelize(List(("tom",1),("tom",2),("jerry",3),("kitty",2)))
    val rdd9 = sc.parallelize(List(("jerry",2),("tom",1),("shark",2)))
//    cogroup 注意cogroup 和 groupByKey的区别
//    println(rdd8.cogroup(rdd9).collect.toBuffer)

    val rdd10 = sc.parallelize(List(1,2,3,4,5))
//   reduce聚合
//    println(rdd10.reduce(_ + _))

    val rdd11 = sc.parallelize(List(("tom",1),("jerry",2),("kitty",3),("shark",1)))
    val rdd12 = sc.parallelize(List(("jerry",2),("tom",3),("shark",2),("kitty",5)))
    val rdd13 = rdd11 union( rdd12)
//    按Key聚合
//    reducyByKey
    val rs13 = rdd13.reduceByKey(_ + _).map(t =>(t._2,t._1)).sortByKey(false).map(t => (t._2,t._1))
//    println(rs13.collect().toBuffer)
//    用sortBy实现按value排序
    val rs14 = rdd13.reduceByKey(_ + _).sortBy(_._2,false)
//    println(rs14.collect().toBuffer)

//   笛卡尔积
//    println(rdd11.cartesian(rdd12).collect.toBuffer)
//    takeOrdered 先排序再获取
    println(rdd11.takeOrdered(4).toBuffer)
   }
}
