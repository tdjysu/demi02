package main.study.scala.com.tdj.study

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

object JsonTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("logDemo").setMaster("local")
    conf.set("spark.testing.memory", "471859200")
    val sc = new SparkContext(conf)

//    val str = "{\"host\":\"td_test\",\"ts\":1486979192345,\"device\":{\"tid\":\"a123456\",\"os\":\"android\",\"sdk\":\"1.0.3\"},\"time\":1501469230058}"

//    val str = "{\"data\":{\"event_type\":\"indoor_location\",\"event_id\":\"init_indoor_location_failed\",\"latitude\":\"0\",\"session_id\":\"e07dca99f3ca0165a27acaa36d7617f7\",\"properties\":{\"callerId\":\"init\",\"message\":\"未扫到beacon\",\"deviceId\":\"766ED6B4-F09C-4B52-8284-4F5C3080E175\",\"errorcode\":\"700001\",\"platform\":\"iOS\",\"status\":\"0\",\"timestamp\":1509692009},\"longitude\":\"0\",\"timestamp\":\"1509692009317\"},\"common_params\":{\"p1\":\"\",\"p2\":\"\",\"p3\":\"iOS\",\"p4\":\"com.dianshang.wanhui\",\"p5\":\"AppStore\",\"p6\":\"766ED6B4-F09C-4B52-8284-4F5C3080E175\",\"p7\":\"iPhone\",\"p8\":\"Apple\",\"p9\":\"iPhone7,2\",\"p10\":\"10.3.3\",\"p12\":\"750\",\"p11\":\"4G\",\"p14\":\"4.23.1\",\"p13\":\"1334\",\"app_key\":\"0948ed8872fb38c4e2b82b842032a34e\",\"p15\":\"4231001\"},\"ffoap_log_get_time\":1509692100,\"@shanghaiTimestamap\":\"2017-11-03 15:00:01\",\"ffoap_dbtype\":\"sdk\"}"
      val str = "{\"data\":{\"event_type\":\"indoor_location\",\"event_id\":\"init_indoor_location_failed\",\"latitude\":\"0\",\"session_id\":\"e07dca99f3ca0165a27acaa36d7617f7\",\"properties\":{\"callerId\":\"init\",\"message\":\"未扫到beacon\",\"deviceId\":\"766ED6B4-F09C-4B52-8284-4F5C3080E175\",\"errorcode\":\"700001\",\"platform\":\"iOS\",\"status\":\"0\",\"timestamp\":1509692009},\"longitude\":\"0\",\"timestamp\":\"1509692009317\"},\"common_params\":{\"p1\":\"\",\"p2\":\"\",\"p3\":\"iOS\",\"p4\":\"com.dianshang.wanhui\",\"p5\":\"AppStore\",\"p6\":\"766ED6B4-F09C-4B52-8284-4F5C3080E175\",\"p7\":\"iPhone\",\"p8\":\"Apple\",\"p9\":\"iPhone7,2\",\"p10\":\"10.3.3\",\"p12\":\"750\",\"p11\":\"4G\",\"p14\":\"4.23.1\",\"p13\":\"1334\",\"app_key\":\"0948ed8872fb38c4e2b82b842032a34e\",\"p15\":\"4231001\"},\"ffoap_log_get_time\":1509692100,\"@shanghaiTimestamap\":\"2017-11-03 15:00:01\",\"ffoap_dbtype\":\"sdk\"}"
//    val str = "{\\\"data\\\":{\\\"event_type\\\":\\\"indoor_location\\\",\\\"event_id\\\":\\\"init_indoor_location_failed\\\",\\\"latitude\\\":\\\"0\\\",\\\"session_id\\\":\\\"e07dca99f3ca0165a27acaa36d7617f7\\\",\\\"properties\\\":{\\\"callerId\\\":\\\"init\\\",\\\"message\\\":\\\"未扫到beacon\\\",\\\"deviceId\\\":\\\"766ED6B4-F09C-4B52-8284-4F5C3080E175\\\",\\\"errorcode\\\":\\\"700001\\\",\\\"platform\\\":\\\"iOS\\\",\\\"status\\\":\\\"0\\\",\\\"timestamp\\\":1509692009},\\\"longitude\\\":\\\"0\\\",\\\"timestamp\\\":\\\"1509692009317\\\"},\\\"common_params\\\":{\\\"p1\\\":\\\"\\\",\\\"p2\\\":\\\"\\\",\\\"p3\\\":\\\"iOS\\\",\\\"p4\\\":\\\"com.dianshang.wanhui\\\",\\\"p5\\\":\\\"AppStore\\\",\\\"p6\\\":\\\"766ED6B4-F09C-4B52-8284-4F5C3080E175\\\",\\\"p7\\\":\\\"iPhone\\\",\\\"p8\\\":\\\"Apple\\\",\\\"p9\\\":\\\"iPhone7,2\\\",\\\"p10\\\":\\\"10.3.3\\\",\\\"p12\\\":\\\"750\\\",\\\"p11\\\":\\\"4G\\\",\\\"p14\\\":\\\"4.23.1\\\",\\\"p13\\\":\\\"1334\\\",\\\"app_key\\\":\\\"0948ed8872fb38c4e2b82b842032a34e\\\",\\\"p15\\\":\\\"4231001\\\"},\\\"ffoap_log_get_time\\\":1509692100,\\\"@shanghaiTimestamap\\\":\\\"2017-11-03 15:00:01\\\",\\\"ffoap_dbtype\\\":\\\"sdk\\\"}\n{\\\"data\\\":{\\\"event_type\\\":\\\"user_behaviour\\\",\\\"event_id\\\":\\\"app_start\\\",\\\"latitude\\\":\\\"\\\",\\\"session_id\\\":\\\"e31d3c0ccb17b1773f03eac559bd965b\\\",\\\"lib_version\\\":\\\"1.0.3\\\",\\\"properties\\\":[],\\\"timestamp\\\":1509692094178,\\\"longitude\\\":\\\"\\\"},\\\"common_params\\\":{\\\"p1\\\":\\\"\\\",\\\"p2\\\":\\\"\\\",\\\"p3\\\":\\\"android\\\",\\\"p4\\\":\\\"com.wanda.app.wanhui\\\",\\\"p5\\\":\\\"10000\\\",\\\"p6\\\":\\\"a6992209-7447-3aa2-8525-75841ef5e1e3\\\",\\\"p7\\\":\\\"sisleylt\\\",\\\"p8\\\":\\\"LENOVO\\\",\\\"p9\\\":\\\"Lenovo S60-t\\\",\\\"p10\\\":\\\"19\\\",\\\"p12\\\":\\\"720\\\",\\\"p11\\\":\\\"WIFI\\\",\\\"p14\\\":\\\"4.23.1.0\\\",\\\"app_key\\\":\\\"5ad19736a1fed662c474319f94260299\\\",\\\"p13\\\":\\\"1280\\\",\\\"p15\\\":\\\"423000103\\\"},\\\"ffoap_log_get_time\\\":1509692103,\\\"@shanghaiTimestamap\\\":\\\"2017-11-03 15:00:01\\\",\\\"ffoap_dbtype\\\":\\\"sdk\\\"}"

    val jsonS = JSON.parseObject(str)
    // 获取一级key对象
    val dataObj = jsonS.getJSONObject("data")
    // 获取二级key对象
    val propObj = dataObj.getJSONObject("properties")
     println(propObj.get("deviceId"))
    // 获取event_id
    println(dataObj.get("event_id"))
    //获取一级key对象
    val paraObj = jsonS.getJSONObject("common_params")


  }
}
