package com.hashmapinc.tempus

import org.apache.spark.streaming._
import org.apache.spark.{SparkConf}
import org.apache.spark.sql.SparkSession

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.log4j.Logger

object ToKudu {
  val KUDU_QUICKSTART_CONNECTION_URL = "jdbc:impala://192.168.56.101:21050/kudu_witsml"
  val KUDU_QUICKSTART_USER_ID = "demo"
  val KUDU_QUICKSTART_PASSWORD = "demo"
  val log = Logger.getLogger(ToKudu.getClass)
  
  def streamDataFromKafkaToKudu(kafka: String, topics: Array[String], kuduUrl: String=KUDU_QUICKSTART_CONNECTION_URL, userId: String=KUDU_QUICKSTART_USER_ID, password: String=KUDU_QUICKSTART_PASSWORD, level: String="WARN"): Unit = {
    val kafkaParams = Map[String, Object](
     "bootstrap.servers" -> kafka,
     "key.deserializer" -> classOf[StringDeserializer],
     "value.deserializer" -> classOf[StringDeserializer],
     "group.id" -> "DEFAULT_GROUP_ID",
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
    log.info("kafka stream is alright")    
    
    //Stream could be empty but that is perfectly okay
    val values = stream.map(record => record.value()).map(s=>s.trim().replace("[", "")).map(s=>s.replace("]", "")).flatMap(s=>s.split("%!%"))

    val records = values.transform(rdd=>{
        if (rdd.isEmpty()) { 
          spark.sparkContext.emptyRDD[DeviceTsDS]
        } else {
          val ds=spark.read.json(rdd).as[DeviceTsDS]
          ds.rdd
        }
    })

    records.foreachRDD(rdd =>{
      if (!rdd.isEmpty()) {
        rdd.foreachPartition { p =>
          val con = ImpalaWrapper.getImpalaConnection(kuduUrl, userId, password)
          
          p.foreach(r => {
            val stmt = ImpalaWrapper.getUpsert(con, r)
            ImpalaWrapper.upsert(con, stmt, r)
            stmt.close()
          })

          ImpalaWrapper.closeConnection(con)
        }
      }
    })
                    
    ssc.start()
    ssc.awaitTermination()
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
      System.out.println("Assuming kuduUrl userId password and logging level defined.")
      ToKudu.streamDataFromKafkaToKudu(args(0), args(1).split(","), args(2), args(3), args(4), args(5))
    }
    else {
      System.out.println("Defaulting kuduUrl userId password and logging level.")
      ToKudu.streamDataFromKafkaToKudu(args(0), args(1).split(","))
    }
    //var s = "1508929960000"
    //System.out.println("s="+s)
    //System.out.println(s.toLong)
  }
}
