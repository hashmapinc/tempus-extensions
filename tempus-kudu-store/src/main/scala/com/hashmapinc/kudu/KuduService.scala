package com.hashmapinc.kudu

import java.sql.{Connection, DriverManager}

import com.hashmapinc.util.TempusKuduConstants.{OFFSETMGR, upsertSQLMap}
import com.hashmapinc.util.TempusUtils
import org.apache.kafka.common.TopicPartition

/**
  * @author Mitesh Rathore
  */
object KuduService {

  var driverLoaded: Boolean = false

  def getImpalaConnection(connectionURL: String, userId: String, password: String) : Connection = {
    val JDBCDriver = "com.cloudera.impala.jdbc4.Driver"

    if (!driverLoaded) {
      Class.forName(JDBCDriver).newInstance()
       driverLoaded = true;
    }
    val impalaConnection = DriverManager.getConnection(connectionURL, userId, password)
    impalaConnection
  }

  def closeConnection(con: Connection): Unit= {
    if (con != null) {
      con.close()
    }
    TempusUtils.INFO("disconnected")
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

  def getOffsets(con: Connection,topicName:String,groupId:String) :Map[TopicPartition,Long] ={
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

}
