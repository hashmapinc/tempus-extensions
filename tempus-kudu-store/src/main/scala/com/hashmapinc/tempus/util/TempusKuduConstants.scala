package com.hashmapinc.tempus.util

import java.text.SimpleDateFormat
import java.util.Calendar

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

}
