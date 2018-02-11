package com.hashmapinc.tempus

import org.apache.spark.streaming._
import org.apache.spark.{SparkConf}
import org.apache.spark.sql.SparkSession

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.util.parsing.json._

import org.apache.log4j.{Logger,Level}

object ToKudu {
  val KUDU_QUICKSTART_CONNECTION_URL = "jdbc:impala://192.168.56.101:21050/kudu_witsml"
  val KUDU_QUICKSTART_USER_ID = "demo"
  val KUDU_QUICKSTART_PASSWORD = "demo"
  val logLevelMap = Map("INFO"->Level.INFO, "WARN"->Level.WARN, "DEBUG"->Level.DEBUG)
  val ID = "id"
  val TSDS = "tsds"
  val VALUE = "value"
  val NAMEWELL = "nameWell"
  val KEY1 = """"id": """
  val KEY2 = """"tsds": """
  val KEY3 = """"value": """
  val log = Logger.getLogger(ToKudu.getClass)
  
  def streamDataFromKafkaToKudu(kafka: String, topics: Array[String], kuduUrl: String=KUDU_QUICKSTART_CONNECTION_URL, userId: String=KUDU_QUICKSTART_USER_ID, password: String=KUDU_QUICKSTART_PASSWORD, level: String="WARN"): Unit = {
    log.setLevel(logLevelMap(level))
    ImpalaWrapper.setLogLevel(logLevelMap(level))
    val kafkaParams = Map[String, Object](
     "bootstrap.servers" -> kafka,
     "key.deserializer" -> classOf[StringDeserializer],
     "value.deserializer" -> classOf[StringDeserializer],
     "group.id" -> topics(0), //"DEFAULT_GROUP_ID",
     "auto.offset.reset" -> "latest",
     "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("FromKafkaToKudu")
    val ssc = new StreamingContext(sparkConf, Minutes(1))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    ssc.sparkContext.setLogLevel("WARN")

    val stream = KafkaUtils
                    .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    assert((stream != null), ERROR("Kafka stream is not available. Check your Kafka setup."))

    //Stream could be empty but that is perfectly okay
    val values  = stream.map(_.value().replace("[", "").replace("]", ""))
                        .flatMap(_.split("%!%")).map(_.trim())
                        .filter(_.length>0)                     //Ignore empty lines
                        .map(toMap(_))
                        .filter(isNonEmptyRecord(_))            //Ignore empty records - id for growing objects and nameWell for attributes

    values.foreachRDD(rdd =>{
      if (!rdd.isEmpty()) {
        rdd.foreachPartition { p =>
          val con = ImpalaWrapper.getImpalaConnection(kuduUrl, userId, password)
          
          p.foreach(r => {
            val stmt = ImpalaWrapper.getUpsert(con, r)
            ImpalaWrapper.upsert(con, stmt, r)
            if (stmt!=null) stmt.close()
          })

          ImpalaWrapper.closeConnection(con)
        }
      }
    })
                    
    ssc.start()
    ssc.awaitTermination()
  }

  def isNonEmptyRecord(record: Map[String, String]): Boolean = {
    //Ignore empty records - id for growing objects and nameWell for attributes
    var result = record.getOrElse(ID, null);
    if (result == null)
      result = record.getOrElse(NAMEWELL, null)
    if (result == null) {
      false;
    }
    true;
  }

  def toMap(record: String): Map[String, String]= {
    var result = JSON.parseRaw(record).getOrElse(null)
    if (result == null) {
      var (key1Index, key2Index, key3Index) = (record.indexOf(KEY1), record.indexOf(KEY2), record.indexOf(KEY3))
      if (key1Index>0 && key2Index>0 && key3Index>0) {
        Map(
            ID    -> record.substring(record.indexOf(KEY1)+KEY1.length+1, record.indexOf(KEY2)-3),
            TSDS  -> record.substring(record.indexOf(KEY2)+KEY2.length+1, record.indexOf(KEY3)-3),
            VALUE -> record.substring(record.indexOf(KEY3)+KEY3.length+1, record.length-2))
      } else {
        WARN(s"Data string found to be null - <${record}>")
        Map(ID -> null, TSDS -> null, VALUE -> null)
      }
    } else {
      var map = result.asInstanceOf[JSONObject].obj.asInstanceOf[Map[String, String]]
      if (map.getOrElse(NAMEWELL, null)==null && map.getOrElse(VALUE, null)==null) {
        WARN(s"Data does not have value field - <${record}>")
        map = Map(ID -> null, TSDS -> null, VALUE -> null)
      }
      map
    }
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
  
 
  def main(args: Array[String]) : Unit = {
    if (args.length>5) {
      ToKudu.streamDataFromKafkaToKudu(args(0), args(1).split(","), args(2), args(3), args(4), args(5))
    }
    else {
      ToKudu.streamDataFromKafkaToKudu(args(0), args(1).split(","))
    }
  }
}
