package com.hashmapinc.tempus

import java.io.FileInputStream
import java.util.Properties

import com.hashmapinc.tempus.util.TempusKuduConstants
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.log4j.{Level, Logger}
import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.util.parsing.json.{JSON, JSONObject}

/**
  * @author Mitesh Rathore
  */
object PutAttributes {


  val WELLINFO  = "WELLINFO"
  val WELLBOREINFO  = "WELLBOREINFO"
  val RIGINFO  = "RIGINFO"
  val TEMPUS_HINT="tempus.hint"
  val TEMPUS_NAMEWELL="tempus.nameWell"

  var driverLoaded: Boolean = false

  val upsertSQLMap = Map(
    WELLINFO  -> "UPSERT INTO well (namewell, operator, state, county, country, timezone, numapi, statuswell, dtimspud,ekey,well_government_id,loadtime) values (?,?,?,?,?,?,?,?,?,?,?,?)",
    WELLBOREINFO  -> "UPSERT INTO wellbore (namewell,namewellbore,statuswellbore,loadtime) values (?,?,?,?)",
    RIGINFO  -> "UPSERT INTO rig (namewell,namewellbore,namerig,ownerrig,dtimstartop,loadtime) values (?,?,?,?,?,?)"
  )
  val log = Logger.getLogger(PutAttributes.getClass)

  def streamDataFromKafkaToKudu(kafka: String, topics: Array[String], kuduUrl: String, userId: String, password: String, level: String="ERROR"): Unit = {
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
    /* val values = stream.map(record => record.value())
     val records = values.transform(rdd=>{
       val ds=spark.read.json(rdd)
       ds.show()
       ds.rdd
     })*/

    val values  = stream.map(_.value())
      .filter(_.length>0)                     //Ignore empty lines
      .map(toMap(_))
      .filter(isNonEmptyRecord(_))



    values.foreachRDD(rdd =>{
      if (!rdd.isEmpty()) {
        rdd.foreachPartition { p =>
          val con =  getImpalaConnection(kuduUrl, userId, password)

          p.foreach(r => {
            upsertAttributeInfo(con, r)

          })

           closeConnection(con)
        }
      }
    })


    def upsertAttributeInfo(con: Connection, rec: Map[String, String]) = {
      DEBUG("Start upsertAttributeInfo")
      val wellName = rec.getOrElse("nameWell", "")
      val wellboreName = rec.getOrElse("nameWellbore", "")
      val rigName = rec.getOrElse("nameRig", "")
      val timeZone = rec.getOrElse("timeZone", "")
      val statusWell = rec.getOrElse("statusWell", "")


      if(!wellName.isEmpty && !timeZone.isEmpty && !statusWell.isEmpty){
        var wellInfo = "WELLINFO";
        val stmt =  con.prepareStatement(upsertSQLMap.getOrElse(wellInfo, null))
        upsertWellInfo(stmt, rec)
      }

      if(!wellboreName.isEmpty){
        var wellboreInfo = "WELLBOREINFO";
        val stmt =  con.prepareStatement(upsertSQLMap.getOrElse(wellboreInfo, null))
        upsertWellboreInfo(stmt, rec)
      }

      if(!rigName.isEmpty){
        var rigInfo = "RIGINFO";
        val stmt =  con.prepareStatement(upsertSQLMap.getOrElse(rigInfo, null))
        upsertRigInfo(stmt, rec)
      }

      if(!rigName.isEmpty){
        var rigInfo = "RIGINFO";
        val stmt =  con.prepareStatement(upsertSQLMap.getOrElse(rigInfo, null))
        upsertRigInfo(stmt, rec)
      }

      DEBUG("End upsertAttributeInfo")
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
        stmt.setString(12,TempusKuduConstants.getCurrentTime)
        try{
          stmt.executeUpdate()
          stmt.close()
        }catch{
          case exp: Exception => ERROR(" Error while populating Well data => "+exp.printStackTrace())
        }
      }
    }
    def upsertWellboreInfo(stmt: PreparedStatement, wellboreInfo: Map[String, String]): Unit = {
      if(stmt != null && wellboreInfo.getOrElse("nameWellbore", null)!=null && wellboreInfo.getOrElse("nameWell", null)!=null){
        stmt.setString(1, wellboreInfo.getOrElse("nameWell", ""))
        stmt.setString(2, wellboreInfo.getOrElse("nameWellbore", ""))
        stmt.setString(3, wellboreInfo.getOrElse("statusWellbore", ""))
        stmt.setString(4,TempusKuduConstants.getCurrentTime)

        try{
          stmt.executeUpdate()
          stmt.close()
        }catch{
          case exp: Exception => ERROR(" Error while populating Wellbore data => "+exp.printStackTrace())
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
        stmt.setString(6,TempusKuduConstants.getCurrentTime)

        try{
          stmt.executeUpdate()
          stmt.close()
        }catch{
          case exp: Exception => ERROR(" Error while populating Rig data => "+exp.printStackTrace())
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }


  def isNonEmptyRecord(record: Map[String, String]): Boolean = {
    //var result = record.getOrElse(TEMPUS_HINT, null);
    var result=record.getOrElse(TEMPUS_NAMEWELL, null);
    if (result==null) {
      DEBUG(s"Returning false => record with no special keys: ${record.toString()}")
      return false

    }
    DEBUG(s"Returning true => record with special keys: ${record.toString()}")
    true
  }

  def toMap(record: String): Map[String, String]= {
    var result = JSON.parseRaw(record).getOrElse(null)
    if (result == null) {
      WARN(s"Record could not be parsed as a JSON object: ${record}")
      Map()
    } else {
      var map = result.asInstanceOf[JSONObject].obj.asInstanceOf[Map[String, String]]
      // WARN(s"  map   : ${map}")
      map
    }
  }

  def main(args: Array[String]) : Unit = {
    var kafkaUrl = ""
    var kuduConnectionUrl = ""
    var kuduConnectionUser = ""
    var kuduConnectionPassword = ""
    var attributeTopicName = ""

    try{
      val prop = new Properties()
      prop.load(new FileInputStream("kudu_witsml.properties"))

      kafkaUrl = prop.getProperty(TempusKuduConstants.KAFKA_URL_PROP)
      kuduConnectionUrl = prop.getProperty(TempusKuduConstants.KUDU_IMPALA_CONNECTION_URL_PROP)
      kuduConnectionUser = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_USER_PROP)
      kuduConnectionPassword = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_PASSWORD_PROP)
      attributeTopicName = prop.getProperty(TempusKuduConstants.TOPIC_ATTRIBUTE_PROP)

      log.info(" kafkaUrl  --- >> "+kafkaUrl)
      log.info(" attributeTopicName  --- >> "+attributeTopicName)
      log.info(" kuduConnectionUrl --- >> "+kuduConnectionUrl)
      log.info(" kuduConnectionUser --- >> "+kuduConnectionUser)
      log.info(" kuduConnectionPassword --- >> "+kuduConnectionPassword)

      if(kafkaUrl.isEmpty || attributeTopicName.isEmpty || kuduConnectionUrl.isEmpty || kuduConnectionUser.isEmpty || kuduConnectionPassword.isEmpty){
        log.info("  <<<--- kudu_witsml.properties file should be presented at classpath location with following properties " +
          "kudu.db.url=jdbc:impala://<HOST_IP>:<PORT>/<DATABASE_SCHEMA>\nkudu.db.user=demo\nkudu.db.password=demo\nkafka.url=kafka:9092\n" +
          "topic.witsml.attribute=well-attribute-data --- >> ")
      }
      else{
        streamDataFromKafkaToKudu(kafkaUrl, Array(attributeTopicName),kuduConnectionUrl,kuduConnectionUser,kuduConnectionPassword)
      }


    }catch{
      case  exp : Exception => exp.printStackTrace()
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

  def getImpalaConnection(connectionURL: String, userId: String, password: String) : Connection = {
    val JDBCDriver = "com.cloudera.impala.jdbc4.Driver"

    if (!driverLoaded) {
      Class.forName(JDBCDriver).newInstance()
      driverLoaded = true;
    }
    val impalaConnection = DriverManager.getConnection(connectionURL, userId, password)
    INFO("connection to db done")
    impalaConnection
  }

  def closeConnection(con: Connection): Unit= {
    if (con != null) {
      con.close()
    }
    INFO("disconnected")
  }

}
