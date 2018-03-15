package com.hashmapinc.util

import java.text.SimpleDateFormat
import java.util.Calendar
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



  def isEmpty(inputString:String) :Boolean = {
    return inputString == null || inputString.trim().length() == 0
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
