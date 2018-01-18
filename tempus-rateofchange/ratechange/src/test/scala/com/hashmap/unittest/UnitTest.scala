package com.hashmap.unittest

import org.scalatest.FlatSpec
import com.hashmap.tempus.WaterLevelPredictor
import com.hashmap.tempus.WaterLevelPredictorAM
import com.hashmap.tempus.ThingsboardPublisher

class UnitTest extends FlatSpec {
  "Single record " should " require no computation " in {
    //Returned value is just the ts, average-rate, and current-level
    assert(WaterLevelPredictor.getRunningAverage("123,1000,10.0123")===(1000,0.0,10.0123))
  }

  it should " compute running average rate for multiple records." in {
      assert(WaterLevelPredictor.getRunningAverage("123,1000,10.0123,123,7000,10.134,123,13000,10.145")===(13000,0.011058333333333318,10.145))
  }
  
  it should " contruct json object " in {
    assert(ThingsboardPublisher.toDataJson("123", 13000, 3458).toString().equalsIgnoreCase("""{"Tank 123":[{"ts":13000,"values":{"timeToFill":3458}}]}"""))
  }

  it should " create vector of current-levels for arima model." in {
      assert(WaterLevelPredictorAM.getWaterlevelValues(Array[String]("10.0123,10.134,10.145"))===Array[Double](10.0123,10.134,10.145))
  }
  
}