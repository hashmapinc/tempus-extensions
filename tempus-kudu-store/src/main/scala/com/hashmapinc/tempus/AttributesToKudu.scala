package com.hashmapinc.tempus

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._


/**
  * @author Mitesh Rathore
  */
object AttributesToKudu {
  val KUDU_QUICKSTART_CONNECTION_URL = "jdbc:impala://192.168.56.101:21050/kudu_witsml"
  val KUDU_QUICKSTART_USER_ID = "demo"
  val KUDU_QUICKSTART_PASSWORD = "demo"
  val log = Logger.getLogger(AttributesToKudu.getClass)
  
  def streamDataFromKafkaToKudu(kafka: String, topics: Array[String], kuduUrl: String=KUDU_QUICKSTART_CONNECTION_URL, userId: String=KUDU_QUICKSTART_USER_ID, password: String=KUDU_QUICKSTART_PASSWORD, level: String="ERROR"): Unit = {
    val kafkaParams = Map[String, Object](
     "bootstrap.servers" -> kafka,
     "key.deserializer" -> classOf[StringDeserializer],
     "value.deserializer" -> classOf[StringDeserializer],
     "group.id" -> "DEFAULT_GROUP_ID",
     "auto.offset.reset" -> "latest",
     "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val sparkConf = new SparkConf().setAppName("AttributesKafkaToKudu")
    val ssc = new StreamingContext(sparkConf, Minutes(1))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    ssc.sparkContext.setLogLevel("ERROR")

    val stream = KafkaUtils
                    .createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    assert((stream != null), log.error("Kafka stream is not available. Check your Kafka setup."))
    log.info("kafka stream is alright")    
    
    //Stream could be empty but that is perfectly okay
    val values = stream.map(record => record.value())
    val records = values.transform(rdd=>{
    val ds=spark.read.json(rdd)
    ds.rdd
    })

    records.foreachRDD(rdd =>{
      if (!rdd.isEmpty()) {
          rdd.foreachPartition { p =>
            val con = ImpalaWrapper.getImpalaConnection(kuduUrl, userId, password)
          p.foreach(r => {
            // Generate the schema based on the string of schema
            //Retrieve the attribute values fro the schema fileds
            val nameWell  = r.get(r.schema.indexOf(new StructField("nameWell",StringType,true))).toString

            var country = ""
            if(r.schema.indexOf(new StructField("country",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("country",StringType,true))) != null)
              country = r.get(r.schema.indexOf(new StructField("country",StringType,true))).toString
            }

            var state = ""
            if(r.schema.indexOf(new StructField("state",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("state",StringType,true))) != null)
              state = r.get(r.schema.indexOf(new StructField("state",StringType,true))).toString
            }

            var county = ""
            if(r.schema.indexOf(new StructField("county",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("county",StringType,true))) != null)
              county = r.get(r.schema.indexOf(new StructField("county",StringType,true))).toString
            }

            var timeZone = ""
            if(r.schema.indexOf(new StructField("timeZone",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("timeZone",StringType,true))) != null)
              timeZone = r.get(r.schema.indexOf(new StructField("timeZone",StringType,true))).toString
            }

            var operator = ""
            if(r.schema.indexOf(new StructField("operator",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("operator",StringType,true))) != null)
              operator = r.get(r.schema.indexOf(new StructField("operator",StringType,true))).toString
            }

            var numAPI = ""
            if(r.schema.indexOf(new StructField("numAPI",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("numAPI",StringType,true))) != null)
              numAPI = r.get(r.schema.indexOf(new StructField("numAPI",StringType,true))).toString
            }

            var dtimSpud = ""
            if(r.schema.indexOf(new StructField("dtimSpud",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("dtimSpud",StringType,true))) != null)
              dtimSpud = r.get(r.schema.indexOf(new StructField("dtimSpud",StringType,true))).toString
            }


            var statusWell = ""
            if(r.schema.indexOf(new StructField("statusWell",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("statusWell",StringType,true))) != null)
              statusWell = r.get(r.schema.indexOf(new StructField("statusWell",StringType,true))).toString
            }
            var nameWellbore = ""
            if(r.schema.indexOf(new StructField("nameWellbore",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("nameWellbore",StringType,true))) != null)
              nameWellbore = r.get(r.schema.indexOf(new StructField("nameWellbore",StringType,true))).toString
            }

            var statusWellbore = ""
            if(r.schema.indexOf(new StructField("statusWellbore",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("statusWellbore",StringType,true))) != null)
              statusWellbore = r.get(r.schema.indexOf(new StructField("statusWellbore",StringType,true))).toString
            }

            var nameRig = ""
            if(r.schema.indexOf(new StructField("nameRig",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("nameRig",StringType,true))) != null)
              nameRig = r.get(r.schema.indexOf(new StructField("nameRig",StringType,true))).toString
            }

            var owner = ""
            if(r.schema.indexOf(new StructField("owner",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("owner",StringType,true))) != null)
              owner = r.get(r.schema.indexOf(new StructField("owner",StringType,true))).toString
            }

            var dtimStartOp = ""
            if(r.schema.indexOf(new StructField("dtimStartOp",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("dtimStartOp",StringType,true))) != null)
              dtimStartOp = r.get(r.schema.indexOf(new StructField("dtimStartOp",StringType,true))).toString
            }

            var ekey = ""
            if(r.schema.indexOf(new StructField("ekey",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("ekey",StringType,true))) != null)
                ekey = r.get(r.schema.indexOf(new StructField("ekey",StringType,true))).toString
            }

            var well_government_id = ""
            if(r.schema.indexOf(new StructField("well_government_id",StringType,true)) != -1){
              if(r.get(r.schema.indexOf(new StructField("well_government_id",StringType,true))) != null)
                well_government_id = r.get(r.schema.indexOf(new StructField("well_government_id",StringType,true))).toString
            }


            val deviceAttr : DeviceAttribute = new DeviceAttribute(nameWell, country, state , county , timeZone, operator,
               numAPI, dtimSpud, statusWell,ekey,well_government_id,
               nameWellbore, statusWellbore, nameRig, owner, dtimStartOp)


             ImpalaWrapper.upsertAttributeInfo(con, deviceAttr)

          })
          ImpalaWrapper.closeConnection(con)
        }
      }
    })
                    
    ssc.start()
    ssc.awaitTermination()
  }
  

  
 
  def main(args: Array[String]) : Unit = {
    log.info(" Kafka URL --- >> "+args(0))
    log.info(" Topic Name --- >> "+args(1))
    AttributesToKudu.streamDataFromKafkaToKudu(args(0), Array(args(1)))
  }
}