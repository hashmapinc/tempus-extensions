package com.hashmapinc.tempus

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

trait Job extends Serializable{

  def process(ssc: StreamingContext): DStream[String] => Unit

  def withStreamingContext: StreamingContext => DStream[String] => Unit = ssc => process(ssc)

  def options: OptionsMap

}

trait StreamingContextHelper extends Serializable{
  def withContext(options: OptionsMap)(f: StreamingContext => Unit){
    val appName = options.getOrElse("appName", "default")
    val duration = options.getOrElse("window", 5000L).asInstanceOf[Long]
    val sparkConf = new SparkConf().setAppName(appName)
    val windowDuration = Milliseconds(duration)
    val ssc = new StreamingContext(sparkConf, windowDuration)
    ssc.sparkContext.setLogLevel("WARN")
    f(ssc)
    ssc.start()
    ssc.awaitTermination()
  }
}
