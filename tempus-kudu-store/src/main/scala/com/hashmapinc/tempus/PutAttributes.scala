package com.hashmapinc.tempus

import java.io.FileInputStream
import java.util.Properties

import com.hashmapinc.util.{TempusKuduConstants, TempusUtils}
import org.apache.spark.streaming.kafka010._
import org.apache.log4j.Logger
import java.sql.{Connection, PreparedStatement}

import com.hashmapinc.kafka.KafkaService
import com.hashmapinc.kudu.KuduService
import com.hashmapinc.spark.SparkService


/**
  * @author Mitesh Rathore
  */
object PutAttributes {


   val log = Logger.getLogger(PutAttributes.getClass)


  def processAttributes(kafkaUrl: String, topics: Array[String], kuduUrl: String, kuduUser:String,kuduPassword:String, groupId:String, timeWindow :String, level: String="WARN"): Unit = {
    val connection =  KuduService.getImpalaConnection(kuduUrl, kuduUser, kuduPassword)

    val spark = SparkService.getSparkSession("PutAttributes")

    var timeWindowInt = 10
    if(!TempusUtils.isEmpty(timeWindow))
      timeWindowInt = timeWindow.toInt

    val streamContext = SparkService.getStreamContext(spark,timeWindowInt)
    streamContext.sparkContext.setLogLevel(level)

    import spark.implicits._
    val stream = KafkaService.readKafka(kafkaUrl, topics, kuduUrl,kuduUser,kuduPassword, groupId, streamContext)

    stream
      .transform {
        rdd =>
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          offsetRanges.foreach(offset => {
            //This method will save the offset to kudu_tempus.offsetmgr table
            KuduService.saveOffsets(connection,topics(0),groupId,offset.untilOffset)
          })
          rdd
      }.map(_.value())
      .filter(_.length>0)                     //Ignore empty lines
      .map(TempusUtils.toMap(_))
      .filter(_.size>0)
      .foreachRDD(rdd =>{
        if (!rdd.isEmpty()) {
          rdd.foreachPartition { p =>
            val con =  KuduService.getImpalaConnection(kuduUrl, kuduUser, kuduPassword)

            p.foreach(r => {
              upsertAttributeInfo(con, r)

            })

            KuduService.closeConnection(con)
          }
        }
      })

    streamContext.start()
    streamContext.awaitTermination()
  }

    def upsertAttributeInfo(con: Connection, rec: Map[String, String]) = {
      TempusUtils.DEBUG("Start upsertAttributeInfo")
      val wellName = rec.getOrElse("nameWell", "")
      val wellboreName = rec.getOrElse("nameWellbore", "")
      val rigName = rec.getOrElse("nameRig", "")
      val timeZone = rec.getOrElse("timeZone", "")
      val statusWell = rec.getOrElse("statusWell", "")


      if(!wellName.isEmpty && !timeZone.isEmpty && !statusWell.isEmpty){
        var wellInfo = "WELLINFO";
        val stmt =  con.prepareStatement(TempusKuduConstants.upsertSQLMap.getOrElse(wellInfo, null))
        upsertWellInfo(stmt, rec)
      }

      if(!wellboreName.isEmpty){
        var wellboreInfo = "WELLBOREINFO";
        val stmt =  con.prepareStatement(TempusKuduConstants.upsertSQLMap.getOrElse(wellboreInfo, null))
        upsertWellboreInfo(stmt, rec)
      }

      if(!rigName.isEmpty){
        var rigInfo = "RIGINFO";
        val stmt =  con.prepareStatement(TempusKuduConstants.upsertSQLMap.getOrElse(rigInfo, null))
        upsertRigInfo(stmt, rec)
      }

      if(!rigName.isEmpty){
        var rigInfo = "RIGINFO";
        val stmt =  con.prepareStatement(TempusKuduConstants.upsertSQLMap.getOrElse(rigInfo, null))
        upsertRigInfo(stmt, rec)
      }

      TempusUtils.DEBUG("End upsertAttributeInfo")
    }

    def upsertWellInfo(stmt: PreparedStatement, deviceAttr: Map[String, String]): Unit = {
      if(stmt != null){
        stmt.setString(1, deviceAttr.getOrElse("nameWell", ""))
        stmt.setString(2, deviceAttr.getOrElse("operator", ""))
        stmt.setString(3, deviceAttr.getOrElse("state", ""))
        stmt.setString(4, deviceAttr.getOrElse("county", ""))
        stmt.setString(5, deviceAttr.getOrElse("country", ""))
        stmt.setString(6, deviceAttr.getOrElse("timeZone", ""))
        stmt.setString(7, deviceAttr.getOrElse("numAPI", ""))
        stmt.setString(8, deviceAttr.getOrElse("statusWell", ""))
        stmt.setString(9, deviceAttr.getOrElse("dtimSpud", ""))
        stmt.setString(10, deviceAttr.getOrElse("ekey", ""))
        stmt.setString(11, deviceAttr.getOrElse("well_government_id", ""))
        stmt.setString(12, deviceAttr.getOrElse("surface_latitude", ""))
        stmt.setString(13, deviceAttr.getOrElse("surface_longitude", ""))
        stmt.setString(14,TempusUtils.getCurrentTime)
        try{
          stmt.executeUpdate()
          stmt.close()
        }catch{
          case exp: Exception => TempusUtils.ERROR(" Error while populating Well data => "+exp.printStackTrace())
        }
      }
    }
    def upsertWellboreInfo(stmt: PreparedStatement, wellboreInfo: Map[String, String]): Unit = {
      if(stmt != null && wellboreInfo.getOrElse("nameWellbore", null)!=null && wellboreInfo.getOrElse("nameWell", null)!=null){
        stmt.setString(1, wellboreInfo.getOrElse("nameWell", ""))
        stmt.setString(2, wellboreInfo.getOrElse("nameWellbore", ""))
        stmt.setString(3, wellboreInfo.getOrElse("statusWellbore", ""))
        stmt.setString(4,TempusUtils.getCurrentTime)

        try{
          stmt.executeUpdate()
          stmt.close()
        }catch{
          case exp: Exception => TempusUtils.ERROR(" Error while populating Wellbore data => "+exp.printStackTrace())
        }
      }

    }

    def upsertRigInfo(stmt: PreparedStatement, rigInfo: Map[String, String]): Unit = {
      if(stmt != null && rigInfo.getOrElse("nameWellbore", null)!=null && rigInfo.getOrElse("nameWell", null)!=null && rigInfo.getOrElse("nameRig", null)!=null){
        stmt.setString(1, rigInfo.getOrElse("nameWell", ""))
        stmt.setString(2, rigInfo.getOrElse("nameWellbore", ""))
        stmt.setString(3, rigInfo.getOrElse("nameRig", ""))
        stmt.setString(4, rigInfo.getOrElse("ownerRig", ""))
        stmt.setString(5, rigInfo.getOrElse("dtimStartOp", ""))
        stmt.setString(6,TempusUtils.getCurrentTime)

        try{
          stmt.executeUpdate()
          stmt.close()
        }catch{
          case exp: Exception => TempusUtils.ERROR(" Error while populating Rig data => "+exp.printStackTrace())
        }
      }
    }




  def isNonEmptyRecord(record: Map[String, String]): Boolean = {
    //var result = record.getOrElse(TEMPUS_HINT, null);
    var result=record.getOrElse(TempusKuduConstants.TEMPUS_NAMEWELL, null);
    if (result==null) {
      TempusUtils.DEBUG(s"Returning false => record with no special keys: ${record.toString()}")
      return false

    }
    TempusUtils.DEBUG(s"Returning true => record with special keys: ${record.toString()}")
    true
  }


  def main(args: Array[String]) : Unit = {
    var kafkaUrl = ""
    var kuduConnectionUrl = ""
    var kuduConnectionUser = ""
    var kuduConnectionPassword = ""
    var topicName = ""
    var logLevel = ""
    var groupId = ""
    var timeWindow = ""


    try{
      val prop = new Properties()
      prop.load(new FileInputStream("kudu_witsml.properties"))

      kafkaUrl = prop.getProperty(TempusKuduConstants.KAFKA_URL_PROP)
      kuduConnectionUrl = prop.getProperty(TempusKuduConstants.KUDU_IMPALA_CONNECTION_URL_PROP)
      kuduConnectionUser = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_USER_PROP)
      kuduConnectionPassword = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_PASSWORD_PROP)
      topicName = prop.getProperty(TempusKuduConstants.TOPIC_ATTRIBUTE_PROP)
      logLevel = prop.getProperty(TempusKuduConstants.LOG_LEVEL)
      groupId =  prop.getProperty(TempusKuduConstants.ATTRIBUTE_KAFKA_GROUP)
      timeWindow =  prop.getProperty(TempusKuduConstants.ATTRIBUTE_TIME_WINDOW)

      log.info(" kafkaUrl  --- >> "+kafkaUrl)
      log.info(" topicName  --- >> "+topicName)
      log.info(" kuduConnectionUrl --- >> "+kuduConnectionUrl)
      log.info(" kuduConnectionUser --- >> "+kuduConnectionUser)
      log.info(" kuduConnectionPassword --- >> "+kuduConnectionPassword)
      log.info(" topicName  --- >> "+topicName)
      log.info(" groupId --- >> "+groupId)
      log.info(" timeWindow --- >> "+timeWindow)

      if(TempusUtils.isEmpty(kafkaUrl) || TempusUtils.isEmpty(topicName)  || TempusUtils.isEmpty(kuduConnectionUrl)
        || TempusUtils.isEmpty(kuduConnectionUser)  || TempusUtils.isEmpty(kuduConnectionPassword)){
        log.info("  <<<--- kudu_witsml.properties file should be presented at classpath location with following properties " +
          "kudu.db.url=<HOST_IP>:<PORT>/<DATABASE_SCHEMA>\nkudu.db.user=demo\nkudu.db.password=demo\nkafka.url=kafka:9092\n" +
          "topic.witsml.attribute=well-attribute-data --- >> ")
      }
      else{
        PutAttributes.processAttributes(kafkaUrl, Array(topicName), kuduConnectionUrl,kuduConnectionUser,kuduConnectionPassword,groupId,timeWindow,logLevel)
      }


    }catch{
      case  exp : Exception => exp.printStackTrace()
    }
  }

}
