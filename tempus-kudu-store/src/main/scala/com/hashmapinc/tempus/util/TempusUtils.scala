package com.hashmapinc.tempus.util

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.hashmapinc.tempus.util.TempusKuduConstants.{OFFSETMGR, driverLoaded, upsertSQLMap}
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.{Level, Logger}

import scala.util.parsing.json.{JSON, JSONObject}

/**
  * @author Mitesh Rathore
  */
object TempusUtils {

  val log = Logger.getLogger(TempusUtils.getClass)


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

  def isSpecialKey(key: String): Boolean= {
    if (TempusKuduConstants.specialKeySet.getOrElse(key, null)!=null)
      return true
    return false
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

}
