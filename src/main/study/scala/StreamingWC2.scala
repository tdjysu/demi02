package scala

import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWC2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("streamWC2").setMaster("local[2]")
    conf.set("spark.testing.memory", "536870912")
    conf.set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf,Seconds(2))
    sc.setCheckpointDir("C:\\data\\checkpoint")
    ssc.checkpoint("C:\\data\\checkpoint")
    val dStream = ssc.socketTextStream("localhost",10086)
// word count 累计统计
//    val res = dStream.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc,new HashPartitioner(sc.defaultParallelism),false)

//    通过mapWithState方式累计，效率更高
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
    val res = dStream.flatMap(_.split(" ")).map((_,1)).mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))


    res.print()

    ssc.start()
    ssc.awaitTermination()
  }

  val updateFunc = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }

  val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
    val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (word, sum)
    state.update(sum)
    output
  }
}
