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
  val upsertSQLMap = Map(
        //MESSAGELOG -> "UPSERT INTO message_log (nameWell, nameWellbore, typeMessage, ts, value, mdvalue, mduom, mdbitvalue, mdbituom) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
    log.info("connection to db done")    
    impalaConnection
  }

  def closeConnection(con: Connection): Unit= {
    if (con != null) {
      con.close()
    }
    log.info("disconnected")    
  }
  
  def getUpsert(con: Connection, rec: DeviceTsDS): PreparedStatement = {
    val hint = rec.id.split(":")(0).toUpperCase()
    log.info("upsert for = "+hint)    
    con.prepareStatement(upsertSQLMap.getOrElse(hint, null))
  }

  def upsertTimeLog(stmt: PreparedStatement, rec: DeviceTsDS, fields: Array[String]): Unit = {
    val cal = java.util.Calendar.getInstance()
    cal.setTimeInMillis(rec.tsds.toLong)
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(cal.getTime())
    stmt.setString(1, fields(3))
    stmt.setString(2, fields(2))
    stmt.setString(3, fields(1))
    stmt.setString(4, fields(0))
    stmt.setString(5, sdf.toString())
    stmt.setDouble(6, rec.value.toDouble)
    stmt.setString(7, rec.value)
    stmt.executeUpdate()
  }

  def upsertMessageLog(stmt: PreparedStatement, rec: DeviceTsDS, fields: Array[String]): Unit = {
    val cal = java.util.Calendar.getInstance()
    cal.setTimeInMillis(rec.tsds.toLong)
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(cal.getTime())
    stmt.setString(1, fields(3))
    stmt.setString(2, fields(2))
    stmt.setString(3, fields(1))
    stmt.setString(4, fields(0))
    stmt.setString(5, sdf.toString())
    stmt.setDouble(6, 0)
    stmt.setString(7, rec.value)
    /*
    stmt.setString(3, fields(0))
    stmt.setString(4, sdf.toString())
    val values=rec.value.split("@")
    stmt.setString(5, values(0))
    stmt.setDouble(6, values(1).toDouble)
    stmt.setString(7, values(2))
    stmt.setDouble(8, values(1).toDouble)
    stmt.setString(9, values(2))
    */
    stmt.executeUpdate()
  }

  def upsertDepthLog(stmt: PreparedStatement, rec: DeviceTsDS, fields: Array[String]): Unit = {
    var ts = rec.tsds
    if (ts.length()<10) {
      ts = "0000000000".substring(0, (10-ts.length()))+ts
    }
    stmt.setString(1, fields(3))
    stmt.setString(2, fields(2))
    stmt.setString(3, fields(1))
    stmt.setString(4, fields(0))
    stmt.setString(5, ts)
    stmt.setDouble(6, rec.tsds.toDouble)
    stmt.setDouble(7, rec.value.toDouble)
    stmt.setString(8, rec.value)
    stmt.executeUpdate()
  }
  
  def upsert(con: Connection, stmt: PreparedStatement, rec: DeviceTsDS): Unit = {
    val hint = rec.id.split(":")(0).toUpperCase()
    val fields = rec.id.split(":")(1).split("@")
    if (hint.equalsIgnoreCase("DEPTHLOG")) {
      upsertDepthLog(stmt, rec, fields)
    } else
    if (hint.equalsIgnoreCase("TIMELOG")) {
      upsertTimeLog(stmt, rec, fields)
    } else
    if (hint.equalsIgnoreCase("MESSAGELOG")) {
      upsertMessageLog(stmt, rec, fields)
    }
  }

  def upsertAttributeInfo(con: Connection, rec: DeviceAttribute) = {
    val wellName = rec.nameWell
    val wellboreName = rec.nameWellbore
    val rigName = rec.nameRig
    val timeZone = rec.timeZone
    val statusWell = rec.statusWell

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

  }

  def upsertWellInfo(stmt: PreparedStatement, deviceAttr: DeviceAttribute): Unit = {
    //UPSERT INTO well_tempus (namewell, operator, state, county, country, timezone, numapi, statuswell,ekey,well_govt_id, dtimspud) values (?,?,?,?,?,?,?,?,,?,?,?)
     if(stmt != null){
       stmt.setString(1, deviceAttr.nameWell)
       stmt.setString(2, deviceAttr.operator)
       stmt.setString(3, deviceAttr.state)
       stmt.setString(4, deviceAttr.county)
       stmt.setString(5, deviceAttr.country)
       stmt.setString(6, deviceAttr.timeZone)
       stmt.setString(7, deviceAttr.numAPI)
       stmt.setString(8, deviceAttr.statusWell)
       stmt.setString(9, deviceAttr.dtimSpud)
       stmt.setString(10, deviceAttr.ekey)
       stmt.setString(11, deviceAttr.govtWellId)
       stmt.setString(12,getCurrentTime)
       try{
         stmt.executeUpdate()
       }catch{
         case exp: Exception => log.error(" Error while populating Well data => "+exp.printStackTrace())
       }
     }
  }
  def upsertWellboreInfo(stmt: PreparedStatement, wellboreInfo: DeviceAttribute): Unit = {
    //UPSERT INTO wellbore_tempus (namewell,namewellbore,statuswellbore) values (?,?,?)

    if(stmt != null && !wellboreInfo.nameWellbore.isEmpty && !wellboreInfo.nameWell.isEmpty){
      stmt.setString(1, wellboreInfo.nameWell)
      stmt.setString(2, wellboreInfo.nameWellbore)
      stmt.setString(3, wellboreInfo.statusWellbore)
      stmt.setString(4,getCurrentTime)

      try{
        stmt.executeUpdate()
      }catch{
        case exp: Exception => log.error(" Error while populating Wellbore data => "+exp.printStackTrace())
      }
    }

  }

  def upsertRigInfo(stmt: PreparedStatement, rigInfo: DeviceAttribute): Unit = {
    //UPSERT INTO rig_tempus (namewell,namewellbore,namerig,ownerrig,dtimstartop) values (?,?,?,?,?)

    if(stmt != null && !rigInfo.nameWellbore.isEmpty && !rigInfo.nameWell.isEmpty && !rigInfo.nameRig.isEmpty){

      stmt.setString(1, rigInfo.nameWell)
      stmt.setString(2, rigInfo.nameWellbore)
      stmt.setString(3, rigInfo.nameRig)
      stmt.setString(4, rigInfo.owner)
      stmt.setString(5, rigInfo.dtimStartOp)
      stmt.setString(6,getCurrentTime)


      try{
        stmt.executeUpdate()
      }catch{
        case exp: Exception => log.error(" Error while populating Rig data => "+exp.printStackTrace())
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


}
