package com.hashmapinc.tempus

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kudu.spark.kudu._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.util.parsing.json.{JSON, JSONObject}
import scala.collection.JavaConverters._

case class depth_log(namewell: String, namewellbore: String, namelog: String, mnemonic: String, depthstring: String, depth: Double, value: Double, value_str: String)
case class Customer(name: String, age:Integer, city: String)

//--packages org.apache.kudu:kudu-spark2_2.11:1.5.0
object ToKudu2 {
  val log = Logger.getLogger(ToKudu2.getClass)
  val specialKeySet = Map("tempus.tsds"->"tempus.tsds", "tempus.hint"->"tempus.hint", "cs"->"cs", "ss"->"ss")

  def streamDataFromKafkaToKudu(kafka: String, topics: Array[String], kuduUrl: String, level: String="WARN"): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka:9092", //kafka,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", //classOf[StringDeserializer],
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer", //classOf[StringDeserializer],
      "group.id" -> "well-log-ds-data", //topics(0),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("ToKudu2")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
   // val master1 = "192.168.56.101:7051"
  //  val kuduMasters = Seq(master1).mkString(",")
   // val kuduContext = new KuduContext(kuduMasters, sc)

    var kuduTableName = "depth_log"
    /*val kuduOptions: Map[String, String] = Map(
      "kudu.table"  -> kuduTableName,
      "kudu.master" -> kuduMasters)*/

    val ssc =new StreamingContext(sc, Seconds(20))

    val stream = KafkaUtils
      .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    /*
    val customersAppend = Array(
      Customer("bob", 30, "boston"),
      Customer("charlie", 23, "san francisco"))
    val customersAppendDF = sc.parallelize(customersAppend).toDF()
    customersAppendDF.write.options(kuduOptions).mode("append").kudu
    */


    val values  = stream.map(_.value())
                        .filter(_.length>0)                     //Ignore empty lines
                        .map(toMap(_)).filter(_.size>0)
                        .flatMap(toDepthLog(_))            //Ignore empty records - id for growing objects and nameWell for attributes

    values.foreachRDD(rdd =>{
      INFO("before upserting")
      //rdd.toDF().write.options(kuduOptions).mode("append").kudu

      rdd.toDF().write.options(Map("kudu.table" -> "impala::kudu_tempus.depth_log","kudu.master" -> "cmas03-mid-tst.conchoresources.com:7051")).mode("append").kudu

      rdd.toDF().show(false)
      INFO("after upserting")
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def toMap(record: String): Map[String, String]= {
    var result = JSON.parseRaw(record).getOrElse(null)
    if (result == null) {
      WARN(s"Record could not be parsed as a JSON object: ${record}")
      Map()
    } else {
      var map = result.asInstanceOf[JSONObject].obj.asInstanceOf[Map[String, String]]
      map
    }
  }

  def toDepthLog(map: Map[String, String]): Array[depth_log]= {
    val dla: Array[depth_log] = new Array[depth_log](map.size-specialKeySet.size)
    var ds = map("tempus.tsds")
    if (ds.length()<10) {
      ds = "0000000000".substring(0, (10-ds.length()))+ds
    }
    var keyIter = map.keys.toIterator
    var i=0
    while (keyIter.hasNext) {
      val key = keyIter.next()
      if (!isSpecialKey(key)) {
        val nameLog = key.split("@")(1)
        val mnemonic = key.split("@")(0)
        val valuestr = map.getOrElse(key, null)
        var value=0.0
        try {
          value = valuestr.toDouble
        } catch {
          case _ => 0.0
        }
        dla(i)=depth_log(getCsAttribute(map, "nameWell"), getCsAttribute(map, "nameWellbore"), nameLog, mnemonic, ds, map("tempus.tsds").toDouble, value, valuestr)
        i = i+1
      }
    }
    INFO("")
    dla
  }

  def isSpecialKey(key: String): Boolean= {
    if (specialKeySet.getOrElse(key, null)!=null)
      return true
    return false
  }

  def getCsAttribute(rec: Map[String, String], name: String): String = {
    var cs:String = rec.getOrElse("cs", null)
    if (cs==null)
      return ""
    var key=name+"="
    var dataStartIndex=cs.indexOf(key)
    if (dataStartIndex>=0) {
      var dataEndIndex=cs.indexOf(", ", dataStartIndex+key.length)
      if (dataEndIndex<0)
        dataEndIndex=cs.indexOf("}", dataStartIndex)
      if (dataEndIndex>=0) {
        try {
          DEBUG(s"${cs.substring(dataStartIndex+key.length, dataEndIndex)}")
          return cs.substring(dataStartIndex+key.length, dataEndIndex)
        } catch {
          case _ => INFO(s"si=${dataStartIndex},ei=${dataEndIndex},cs=${cs}")
        }
      }
    }
    return ""
  }

  def WARN(s: String): Unit={
    if (log.isEnabledFor(Level.WARN)) {
      log.warn(s)
    }
  }

  def INFO(s: String): Unit={
    if (log.isInfoEnabled()) {
      log.info(s)
    }
  }

  def DEBUG(s: String): Unit={
    if (log.isDebugEnabled()) {
      log.debug(s)
    }
  }

  def ERROR(s: String): Unit={
    log.error(s)
  }

  def main(args: Array[String]): Unit={
    ToKudu2.streamDataFromKafkaToKudu(args(0), args(1).split(","), args(2), args(3))
  }
}
