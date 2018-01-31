package com.hashmapinc.tempus

import java.sql.DriverManager
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable.HashMap

object ImpalaWrapper {
  val MESSAGELOG = "MESSAGELOG"
  val DEPTHLOG = "DEPTHLOG"
  val TIMELOG  = "TIMELOG"
  val upsertSQLMap = Map(
        MESSAGELOG -> "UPSERT INTO message_log (nameWell, nameWellbore, typeMessage, ts, value, mdvalue, mduom, mdbitvalue, mdbituom) values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        DEPTHLOG -> "UPSERT INTO depth_log (nameWell, nameWellbore, nameLog, mnemonic, depthString, depth, value) values (?, ?, ?, ?, ?, ?, ?)",
        TIMELOG  -> "UPSERT INTO time_log (nameWell, nameWellbore, nameLog, mnemonic, ts, value) values (?, ?, ?, ?, ?, ?)"
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
    stmt.executeUpdate()
  }

  def upsertMessageLog(stmt: PreparedStatement, rec: DeviceTsDS, fields: Array[String]): Unit = {
    val cal = java.util.Calendar.getInstance()
    cal.setTimeInMillis(rec.tsds.toLong)
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").format(cal.getTime())
    stmt.setString(1, fields(3))
    stmt.setString(2, fields(2))
    stmt.setString(3, fields(0))
    stmt.setString(4, sdf.toString())
    val values=rec.value.split("@")
    stmt.setString(5, values(0))
    stmt.setDouble(6, values(1).toDouble)
    stmt.setString(7, values(2))
    stmt.setDouble(8, values(1).toDouble)
    stmt.setString(9, values(2))
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
}
