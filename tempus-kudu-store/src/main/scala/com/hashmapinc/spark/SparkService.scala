package com.hashmapinc.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author Mitesh Rathore
  */
object SparkService {

  def getSparkSession(jobName :String) :SparkSession ={
    val spark = SparkSession
      .builder()
      .appName(jobName)
      .getOrCreate()
    spark
  }

  def getStreamContext(spark :SparkSession,window :Int) :StreamingContext ={
    val ssc = new StreamingContext(spark.sparkContext, Seconds(window))
    ssc
  }

}
