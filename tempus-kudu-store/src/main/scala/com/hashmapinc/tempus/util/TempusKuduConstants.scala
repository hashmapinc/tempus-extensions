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

  val specialKeySet = Map("tempus.tsds"->"tempus.tsds", "tempus.hint"->"tempus.hint", "nameWell"->"nameWell", "nameWellbore"->"nameWellbore", "LogName"->"LogName")






}
