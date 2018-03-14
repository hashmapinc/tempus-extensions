package com.hashmapinc.tempus
import java.io.FileInputStream
import java.util.Properties
import com.hashmapinc.tempus.util.{KafkaService, SparkService, TempusKuduConstants, TempusUtils}
import org.apache.log4j.{Logger}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges}
import org.apache.kudu.spark.kudu._

/**
  * @author Mitesh Rathore
  */
object PutTrajectory {



  val log = Logger.getLogger(PutDataInKudu.getClass)

  val groupId = "Trajectory"

  def processTrajectory(kafkaUrl: String, topics: Array[String], kuduUrl: String, kuduTableName:String,impalaKuduUrl:String,kuduUser:String,kuduPassword:String, level: String="WARN"): Unit = {
    val connection =  TempusUtils.getImpalaConnection(impalaKuduUrl, kuduUser, kuduPassword)

    val spark = SparkService.getSparkSession("PutMessage")
    val streamContext = SparkService.getStreamContext(spark,10)
    streamContext.sparkContext.setLogLevel(level)

    import spark.implicits._

    val stream = KafkaService.readKafka(kafkaUrl, topics, impalaKuduUrl,kuduUser,kuduPassword, groupId, streamContext)
    stream
      .transform {
        rdd =>
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          offsetRanges.foreach(offset => {
            //This method will save the offset to kudu_tempus.offsetmgr table
            TempusUtils.saveOffsets(connection,topics(0),groupId,offset.untilOffset)
          })
          rdd
      }.map(_.value())
      .filter(_.length>0)                     //Ignore empty lines
      .map(TempusUtils.toMap(_))
      .filter(_.size>0)
      .map(toTrajectory(_))
      .foreachRDD(rdd =>{
        TempusUtils.INFO("before Trajectory upserting")
        rdd.toDF().write.options(Map("kudu.table" -> kuduTableName,"kudu.master" -> kuduUrl)).mode("append").kudu
        TempusUtils.INFO("after Trajectory upserting")
      })

    streamContext.start()
    streamContext.awaitTermination()
  }


  def toTrajectory(map: Map[String, String]): Trajectory= {

    var nameWell = map("nameWell")
    var nameWellbore = map("nameWellbore")
    var nameTrajectory = map("nameTrajectory")
    var nameTrajectoryStn = map("nameTrajectoryStn")
    var aziVertSectUom = map.getOrElse("aziVertSectUom","")
    var mdUom = map.getOrElse("mdUom","")

    var tvdUom = map.getOrElse("tvdUom","")
    var inclUom = map.getOrElse("inclUom","")
    var aziUom = map.getOrElse("aziUom","")
    var dispNsUom = map.getOrElse("dispNsUom","")

    var dispEwUom = map.getOrElse("dispEwUom","")
    var vertSectUom = map.getOrElse("vertSectUom","")
    var dlsUom = map.getOrElse("dlsUom","")

    var aziVertSectValue  = map.getOrElse("aziVertSectValue","")
    var aziVertSectValueDouble = 0.0
    if(!aziVertSectValue.isEmpty){
      aziVertSectValueDouble = aziVertSectValue.toDouble
    }

    var mdValue = map.getOrElse("mdValue","")
    var mdValueDouble = 0.0
    if(!mdValue.isEmpty){
      mdValueDouble = mdValue.toDouble
    }


    var dispNsVertSecOrigValue = map.getOrElse("dispNsVertSecOrigValue","")
    var dispNsVertSecOrigValueDouble = 0.0
    if(!dispNsVertSecOrigValue.isEmpty){
      dispNsVertSecOrigValueDouble = dispNsVertSecOrigValue.toDouble
    }


    var dispEwVertSecOrigValue = map.getOrElse("dispEwVertSecOrigValue","")
    var dispEwVertSecOrigValueDouble = 0.0
    if(!dispEwVertSecOrigValue.isEmpty){
      dispEwVertSecOrigValueDouble = dispEwVertSecOrigValue.toDouble
    }

    var tvdValue = map.getOrElse("tvdValue","")
    var tvdValueDouble = 0.0
    if(!tvdValue.isEmpty){
      tvdValueDouble = tvdValue.toDouble
    }

    var inclValue = map.getOrElse("inclValue","")
    var inclValueDouble = 0.0
    if(!inclValue.isEmpty){
      inclValueDouble = inclValue.toDouble
    }


    var aziValue = map.getOrElse("aziValue","")
    var aziValueDouble = 0.0
    if(!aziValue.isEmpty){
      aziValueDouble = aziValue.toDouble
    }

    var dispNsValue = map.getOrElse("dispNsValue","")
    var dispNsValueDouble = 0.0
    if(!dispNsValue.isEmpty){
      dispNsValueDouble = dispNsValue.toDouble
    }



    var dispEwValue = map.getOrElse("dispEwValue","")
    var dispEwValueDouble = 0.0
    if(!dispEwValue.isEmpty){
      dispEwValueDouble = dispEwValue.toDouble
    }

    var dlsValue = map.getOrElse("dlsValue","")
    var dlsValueDouble = 0.0
    if(!dlsValue.isEmpty){
      dlsValueDouble = dlsValue.toDouble
    }

    var vertSectValue = map.getOrElse("vertSectValue","")
    var vertSectValueDouble = 0.0
    if(!vertSectValue.isEmpty){
      vertSectValueDouble = vertSectValue.toDouble
    }


    var dispEwVertSecOrigUom = map.getOrElse("dispEwVertSecOrigUom","")
    var aziRef = map.getOrElse("aziRef","")
    var cmnDataDtimCreation = map.getOrElse("cmnDataDtimCreation","")
    var cmnDataDtimLstChange = map.getOrElse("cmnDataDtimLstChange","")
    var trajectoryStnType = map.getOrElse("trajectoryStnType","")

    var dtimStn = map.getOrElse("dtimStn","")


    Trajectory(nameWell, nameWellbore, nameTrajectory, nameTrajectoryStn, aziVertSectValueDouble,aziVertSectUom,
      dispEwVertSecOrigValueDouble,dispEwVertSecOrigUom,dispEwVertSecOrigValueDouble,dispEwVertSecOrigUom,
      aziRef,cmnDataDtimCreation,cmnDataDtimLstChange,trajectoryStnType,mdValueDouble,mdUom,tvdValueDouble,tvdUom,
      inclValueDouble,inclUom,aziValueDouble,aziUom,dispNsValueDouble,dispNsUom,dispEwValueDouble,dispEwUom,
      vertSectValueDouble,vertSectUom,dlsValueDouble,dlsUom,dtimStn,TempusUtils.getCurrentTime)



  }




  def main(args: Array[String]): Unit={

    var kafkaUrl = ""
    var kuduConnectionUrl = ""
    var kuduConnectionUser = ""
    var kuduConnectionPassword = ""
    var topicName = ""
    var logLevel = ""
    var kuduTableName = "impala::kudu_tempus.depth_log"
    var impalaKuduUrl = ""

    try{
      val prop = new Properties()
      prop.load(new FileInputStream("kudu_witsml.properties"))

      kafkaUrl = prop.getProperty(TempusKuduConstants.KAFKA_URL_PROP)
      kuduConnectionUrl = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_URL_PROP)
      kuduConnectionUser = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_USER_PROP)
      kuduConnectionPassword = prop.getProperty(TempusKuduConstants.KUDU_CONNECTION_PASSWORD_PROP)
      logLevel = prop.getProperty(TempusKuduConstants.LOG_LEVEL)
      topicName = prop.getProperty(TempusKuduConstants.TOPIC_TRAJECTORY_PROP)
      impalaKuduUrl = prop.getProperty(TempusKuduConstants.KUDU_IMPALA_CONNECTION_URL_PROP)
      kuduTableName = "impala::"+prop.getProperty(TempusKuduConstants.KUDU_TRAJECTORY_TABLE)


      log.info(" kafkaUrl  --- >> "+kafkaUrl)
      log.info(" topicName  --- >> "+topicName)
      log.info(" kuduConnectionUrl --- >> "+kuduConnectionUrl)
      log.info(" kuduConnectionUser --- >> "+kuduConnectionUser)
      log.info(" kuduConnectionPassword --- >> "+kuduConnectionPassword)

      if(kafkaUrl == null || topicName == null || kuduConnectionUrl== null || kuduConnectionUser== null || kuduConnectionPassword== null){
        log.info("  <<<--- kudu_witsml.properties file should be presented at classpath location with following properties " +
          "kudu.db.url=<HOST_IP>:<PORT>/<DATABASE_SCHEMA>\nkudu.db.user=demo\nkudu.db.password=demo\nkafka.url=kafka:9092\n" +
          "topic.witsml.attribute=well-attribute-data --- >> ")
      }
      else{
        PutTrajectory.processTrajectory(kafkaUrl, Array(topicName), kuduConnectionUrl,kuduTableName,impalaKuduUrl,kuduConnectionUser,kuduConnectionPassword,logLevel)
      }


    }catch{
      case  exp : Exception => exp.printStackTrace()
    }

  }


}
