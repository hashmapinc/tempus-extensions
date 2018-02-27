package com.hashmapinc.tempus

import java.sql.DriverManager
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable.HashMap

object ImpalaWrapper {
  val WELLINFO  = "WELLINFO"
  val WELLBOREINFO  = "WELLBOREINFO"
  val RIGINFO  = "RIGINFO"
  val MESSAGELOG = "MESSAGELOG"
  val DEPTHLOG = "DEPTHLOG"
  val TIMELOG  = "TIMELOG"
  val TEMPUS_HINT="tempus.hint"
  val TEMPUS_TSDS="tempus.tsds"
  val TEMPUS_NAMEWELL="tempus.nameWell"
  val CS="cs"
  val SS="ss"
  val specialKeySet = Map(TEMPUS_TSDS->TEMPUS_TSDS, TEMPUS_NAMEWELL->TEMPUS_NAMEWELL, TEMPUS_HINT->TEMPUS_HINT, CS->CS, SS->SS)
  val upsertSQLMap = Map(
    DEPTHLOG -> "UPSERT INTO depth_log (nameWell, nameWellbore, nameLog, mnemonic, depthString, depth, value, value_str) values (?, ?, ?, ?, ?, ?, ?, ?)",
    TIMELOG  -> "UPSERT INTO time_log (nameWell, nameWellbore, nameLog, mnemonic, ts, value, value_str) values (?, ?, ?, ?, ?, ?, ?)",
    MESSAGELOG -> "UPSERT INTO time_log (nameWell, nameWellbore, nameLog, mnemonic, ts, value, value_str) values (?, ?, ?, ?, ?, ?, ?)",
    WELLINFO  -> "UPSERT INTO well_tempus (namewell, operator, state, county, country, timezone, numapi, statuswell, dtimspud,ekey,well_government_id,loadtime) values (?,?,?,?,?,?,?,?,?,?,?,?)",
    WELLBOREINFO  -> "UPSERT INTO wellbore_tempus (namewell,namewellbore,statuswellbore,loadtime) values (?,?,?,?)",
    RIGINFO  -> "UPSERT INTO rig_tempus (namewell,namewellbore,namerig,ownerrig,dtimstartop,loadtime) values (?,?,?,?,?,?)"
  )
  var driverLoaded: Boolean = false

  val log = Logger.getLogger(ImpalaWrapper.getClass)

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

  def getUpsert(con: Connection, rec: Map[String, String]): PreparedStatement = {
    val hint = rec.getOrElse(TEMPUS_HINT, "NONIDRECORD").toUpperCase()
    val stmt=upsertSQLMap.getOrElse(hint, null)
    if (stmt==null)
      null
    else
      con.prepareStatement(stmt)
  }

  def upsertTimeLog(stmt: PreparedStatement, rec: Map[String, String]): Unit = {
    DEBUG("Start upsertTimeLog")
    val cal = java.util.Calendar.getInstance()
    cal.setTimeInMillis(rec(TEMPUS_TSDS).toLong)
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(cal.getTime())
    stmt.setString(1, getCsAttribute(rec, "nameWell"))
    stmt.setString(2, getCsAttribute(rec, "nameWellbore"))
    stmt.setString(5, sdf.toString())
    var keyIter = rec.keys.toIterator
    while (keyIter.hasNext) {
      val key = keyIter.next()
      if (!isSpecialKey(key)) {
        stmt.setString(3, key.split("@")(1))
        stmt.setString(4, key.split("@")(0))
        val value = rec.getOrElse(key, null)
        try {
          stmt.setDouble(6, value.toDouble)
        } catch {
          case _ => stmt.setDouble(6, 0)
        }
        stmt.setString(7, value)
        stmt.executeUpdate()
      }
    }
    DEBUG("End upsertTimeLog")
  }

  def upsertMessageLog(stmt: PreparedStatement, rec: Map[String, String]): Unit = {
    DEBUG("Start upsertMessageLog")
    val cal = java.util.Calendar.getInstance()
    cal.setTimeInMillis(rec(TEMPUS_TSDS).toLong)
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(cal.getTime())
    stmt.setString(1, getCsAttribute(rec, "nameWell"))
    stmt.setString(2, getCsAttribute(rec, "nameWellbore"))
    stmt.setString(5, sdf.toString())
    var keyIter = rec.keys.toIterator
    while (keyIter.hasNext) {
      val key = keyIter.next()
      if (!isSpecialKey(key)) {
        stmt.setString(3, key)
        stmt.setString(4, key)
        val value = rec.getOrElse(key, null)
        stmt.setString(7, value)
        stmt.setDouble(6, 0)
        stmt.executeUpdate()
      }
    }
    DEBUG("End upsertMessageLog")
  }

  def getCsAttribute(rec: Map[String, String], name: String): String = {
    var cs:String = rec.getOrElse("cs", null)
    if (cs==null)
      return ""
    var key=name+"="
    var dataStartIndex=cs.indexOf(key)+key.length
    var dataEndIndex=cs.indexOf(", ", dataStartIndex)
    if (dataEndIndex<0)
      dataEndIndex=cs.indexOf("}", dataStartIndex)
    DEBUG(s"${cs.substring(dataStartIndex, dataEndIndex)}")
    return cs.substring(dataStartIndex, dataEndIndex)
  }

  def upsertDepthLog(stmt: PreparedStatement, rec: Map[String, String]): Unit = {
    DEBUG("Start upsertDepthLog")
    var ts = rec(TEMPUS_TSDS)
    if (ts.length()<10) {
      ts = "0000000000".substring(0, (10-ts.length()))+ts
    }
    stmt.setString(1, getCsAttribute(rec, "nameWell"))
    stmt.setString(2, getCsAttribute(rec, "nameWellbore"))
    stmt.setString(5, ts)
    stmt.setDouble(6, rec(TEMPUS_TSDS).toDouble)
    var keyIter = rec.keys.toIterator
    while (keyIter.hasNext) {
      val key = keyIter.next()
      if (!isSpecialKey(key)) {
        stmt.setString(3, key.split("@")(1))
        stmt.setString(4, key.split("@")(0))
        val value = rec.getOrElse(key, null)
        try {
          stmt.setDouble(7, value.toDouble)
        } catch {
          case _ => stmt.setDouble(7, 0)
        }
        stmt.setString(8, value)
        stmt.executeUpdate()
      }
    }
    DEBUG("End upsertDepthLog")
  }

  def isSpecialKey(key: String): Boolean= {
    if (specialKeySet.getOrElse(key, null)!=null)
      return true
    return false
  }

  def upsert(con: Connection, stmt: PreparedStatement, rec: Map[String, String]): Unit = {
    if (stmt!=null) {
      val hint = rec(TEMPUS_HINT).toUpperCase()
      if (hint.equalsIgnoreCase("DEPTHLOG")) {
        upsertDepthLog(stmt, rec)
      } else if (hint.equalsIgnoreCase("TIMELOG")) {
        upsertTimeLog(stmt, rec)
      } else if (hint.equalsIgnoreCase("MESSAGELOG")) {
        upsertMessageLog(stmt, rec)
      }
    } else {
      upsertAttributeInfo(con, rec)
    }
  }

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
      stmt.setString(12,getCurrentTime)
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
      stmt.setString(4,getCurrentTime)

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
      stmt.setString(6,getCurrentTime)

      try{
        stmt.executeUpdate()
        stmt.close()
      }catch{
        case exp: Exception => ERROR(" Error while populating Rig data => "+exp.printStackTrace())
      }
    }
  }

  /**
    * Parse Long time to ISO date format
    * @param longTime
    * @return
    */
  def formatTime(longTime : String) : String  = {
    var formatTime :String = null
    try{

      val cal = java.util.Calendar.getInstance()
      cal.setTimeInMillis(longTime.toLong)
      val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(cal.getTime)
      formatTime  = sdf.toString

    }catch{
      case exp: Exception => exp.printStackTrace()
    }

    formatTime
  }

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


  def DEBUG(s: String): Unit={
    if (log.isDebugEnabled()) {
      log.debug(s)
    }
  }

  def INFO(s: String): Unit={
    if (log.isInfoEnabled()) {
      log.info(s)
    }
  }

  def ERROR(s: String): Unit={
    log.error(s)
  }

  def setLogLevel(level: Level): Unit={
    log.setLevel(level)
  }
}
