package com.hashmapinc.util

import org.apache.log4j.{Level, Logger}

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
  val MAX_INFLIGHT_SIZE = 1000

  val DEPTHLOG_KAFKA_GROUP="depthlog.kafka.group"
  val DEPTHLOG_TIME_WINDOW = "depthlog.kafka.window"

  val TIMELOG_KAFKA_GROUP="timelog.kafka.group"
  val TIMELOG_TIME_WINDOW = "timelog.kafka.window"

  val MESSAGELOG_KAFKA_GROUP="messagelog.kafka.group"
  val MESSAGELOG_TIME_WINDOW = "messagelog.kafka.window"

  val TRAJECTORY_KAFKA_GROUP="trajectory.kafka.group"
  val TRAJECTORY_TIME_WINDOW = "trajectory.kafka.window"

  val ATTRIBUTE_KAFKA_GROUP="attribute.kafka.group"
  val ATTRIBUTE_TIME_WINDOW = "attribute.kafka.window"
  val MSE_KAFKA_GROUP="mse.kafka.group"
  val MSE_TIME_WINDOW = "mse.kafka.window"
  val WELLINFO  = "WELLINFO"
  val WELLBOREINFO  = "WELLBOREINFO"
  val RIGINFO  = "RIGINFO"
  val TEMPUS_HINT="tempus.hint"
  val TEMPUS_NAMEWELL="tempus.nameWell"

  val logLevelMap = Map("INFO"->Level.INFO, "WARN"->Level.WARN, "DEBUG"->Level.DEBUG, "ERROR"->Level.ERROR)
  val upsertSQLMap = Map(
    OFFSETMGR  -> "UPSERT INTO offsetmgr (topic_name,group_id,offsetid) values (?,?,?)",
    WELLINFO  -> "UPSERT INTO well (namewell, operator, state, county, country, timezone, numapi, statuswell, dtimspud,ekey,well_government_id,surface_latitude,surface_longitude,loadtime) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    WELLBOREINFO  -> "UPSERT INTO wellbore (namewell,namewellbore,statuswellbore,loadtime) values (?,?,?,?)",
    RIGINFO  -> "UPSERT INTO rig (namewell,namewellbore,namerig,ownerrig,dtimstartop,loadtime) values (?,?,?,?,?,?)"
  )

  val specialKeySet = Map("tempus.tsds"->"tempus.tsds", "tempus.hint"->"tempus.hint", "nameWell"->"nameWell", "nameWellbore"->"nameWellbore", "LogName"->"LogName")






}
