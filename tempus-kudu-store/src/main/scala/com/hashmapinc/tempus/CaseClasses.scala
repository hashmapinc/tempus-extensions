package com.hashmapinc.tempus

case class DeviceTsDS(id: String, tsds: String, value: String)

case class DeviceAttribute(nameWell: String, country: String, state : String, county: String, timeZone:String,
                           operator:String, numAPI : String, dtimSpud : String, statusWell : String, ekey : String, govtWellId : String ,
                           nameWellbore: String, statusWellbore : String, nameRig: String, owner : String, dtimStartOp: String)

trait CaseClasses {
  
}
