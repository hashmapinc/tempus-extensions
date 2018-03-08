package com.hashmapinc.tempus

case class DeviceTsDS(id: String, tsds: String, value: String)

case class DeviceAttribute(nameWell: String, country: String, state : String, county: String, timeZone:String,
                           operator:String, numAPI : String, dtimSpud : String, statusWell : String, ekey : String, govtWellId : String ,
                           nameWellbore: String, statusWellbore : String, nameRig: String, owner : String, dtimStartOp: String)

case class DepthLog(namewell: String, namewellbore: String, namelog: String, mnemonic: String, depthstring: String, depth: Double, value: Double, value_str: String)

case class Trajectory(namewell: String, namewellbore: String, nametrajectory: String, nametrajectorystn: String, azivertsectvalue:Double,azivertsectuom:String,
                      dispnsvertsecorigvalue:Double,dispnsvertsecoriguom:String,dispewvertsecorigvalue:Double,dispewvertsecoriguom:String,
                      aziref:String,cmndatadtimcreation:String,cmndatadtimlstchange:String,typetrajstation:String,mdvalue:Double,mduom:String,tvdvalue:Double,tvduom:String,
                      inclvalue:Double,incluom:String,azivalue:Double,aziuom:String,dispnsvalue:Double,dispnsuom:String,dispewvalue:Double,dispewuom:String,
                      vertsectvalue:Double,vertsectuom:String,dlsvalue:Double,dlsuom:String,dtimstn:String,loadtime:String)

case class TimeLog(namewell: String, namewellbore: String, namelog: String, mnemonic: String, ts: String,  value: Double, value_str: String)

trait CaseClasses {
  
}
