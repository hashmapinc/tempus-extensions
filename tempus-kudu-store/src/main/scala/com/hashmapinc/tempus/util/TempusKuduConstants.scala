package com.hashmapinc.tempus.util

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.kafka.common.TopicPartition

/**
  * @author Mitesh Rathore
  */
object TempusKuduConstants {


  val KUDU_WITSML_PROP = "kudu_witsml.properties"
  val KUDU_CONNECTION_URL_PROP = "kudu.db.url"
  val KUDU_IMPALA_CONNECTION_URL_PROP = "impala.kudu.db.url"


  val KUDU_CONNECTION_USER_PROP = "kudu.db.user"
  val KUDU_CONNECTION_PASSWORD_PROP = "kudu.db.password"
  val KAFKA_URL_PROP = "kafka.url"
  val TOPIC_ATTRIBUTE_PROP = "topic.witsml.attribute"
  val TOPIC_DEPTHLOG_PROP = "topic.witsml.depthlog"
  val TOPIC_TIMELOG_PROP = "topic.witsml.timelog"
  val TOPIC_MESSAGE_PROP = "topic.witsml.message"
  val TOPIC_TRAJECTORY_PROP = "topic.witsml.trajectory"
  val TOPIC_MSE_PROP = "topic.witsml.mse"
  val LOG_LEVEL = "log.level"

  val KUDU_DEPTHLOG_TABLE = "kudu.depth.log"
  val KUDU_TIMELOG_TABLE = "kudu.time.log"
  val KUDU_TRAJECTORY_TABLE = "kudu.trajectory"

  val DEPTHLOG_OBJECTTYPE = "DEPTH"
  val TIMELOG_OBJECTTYPE = "TIME"
  val TRAJECTORY_OBJECTTYPE = "TRAJECTORY"
  val MESSAGE_OBJECTTYPE = "MESSAGE"
  val ATTRIBUTE_OBJECTTYPE = "ATTRIBUTE"

  val TEMPUS_TRAJECTORY="nameTrajectoryStn"

  val TEMPUS_TRAJECTORY_HINT="TRAJECTORY"

  val TEMPUS_MQTT_URL = "tempus.mqtt.url"
  val TEMPUS_MQTT_TOKEN = "tempus.mqtt.token"

  val TEMPUS_MQTT_TOPIC = "tempus.mqtt.topic"

  val MSE_TOR_KEY="mse.key1"
  val MSE_WOB_KEY = "mse.key2"
  val MSE_RPM_KEY = "mse.key3"
  val MSE_ROP_KEY = "mse.key4"

  val MSE_MNEMONIC= "mse.mnemonic"

  val OFFSETMGR  = "OFFSETMGR"

  val GETOFFSETMGR  = "GETOFFSETMGR"

  var driverLoaded: Boolean = false

  val upsertSQLMap = Map(
    OFFSETMGR  -> "UPSERT INTO offsetmgr (topic_name,group_id,offsetid) values (?,?,?)"
  )



  /**
    * Retrieve Current Time
    * @return
    */
  def getCurrentTime : String = {
    val cal = java.util.Calendar.getInstance()
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(cal.getTime())
    var currentDate : String = sdf.toString
    currentDate
  }

  def getFormattedTime (longtime:String) : String = {
    var formattedTime = longtime
    try{
      val calendar = Calendar.getInstance()
      calendar.setTimeInMillis(longtime.toLong)
      formattedTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(calendar.getTime())
    }catch{
      case exp: Exception =>  exp.printStackTrace()
    }

    formattedTime
  }

  def saveOffsets(con: Connection,topicName:String,groupId:String,offsetId:Long) ={

    try{
    val stmt =  con.prepareStatement(upsertSQLMap.getOrElse(OFFSETMGR, null))
    stmt.setString(1, topicName)
    stmt.setString(2, groupId)
    stmt.setString(3, String.valueOf(offsetId))
    stmt.executeUpdate()
    }catch{
      case exp: Exception =>  exp.printStackTrace()
    }

  }

  def getLastCommittedOffsets(con: Connection,topicName:String,groupId:String) :Map[TopicPartition,Long] ={
    var offsetId : String = null;
    var getOffset = "SELECT offsetid FROM offsetmgr WHERE topic_name = '"+topicName +"' and group_id = '"+groupId +"';"
    try{
      val stmt =  con.prepareStatement(getOffset)

      if(stmt != null){
        val rs = stmt.executeQuery()
        while(rs.next()){
          offsetId = rs.getString(1)
        }
      }

    }catch{
      case exp: Exception =>  exp.printStackTrace()
    }

    val fromOffsets = collection.mutable.Map[TopicPartition,Long]()

    if(offsetId!= null){
      fromOffsets += (new TopicPartition(topicName,0) -> offsetId.toLong)
    }

    fromOffsets.toMap

  }



  def getImpalaConnection(connectionURL: String, userId: String, password: String) : Connection = {
    val JDBCDriver = "com.cloudera.impala.jdbc4.Driver"

    if (!driverLoaded) {
      Class.forName(JDBCDriver).newInstance()
      driverLoaded = true;
    }
    val impalaConnection = DriverManager.getConnection(connectionURL, userId, password)
    impalaConnection
  }


}
