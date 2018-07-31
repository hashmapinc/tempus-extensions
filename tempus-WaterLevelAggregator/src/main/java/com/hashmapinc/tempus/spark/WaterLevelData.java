package com.hashmapinc.tempus.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterLevelData implements Serializable {

    private String tankId;
    private long ts;
    private double waterTankLevel;

    public WaterLevelData(String device, double level) {
        this.tankId = device;
        this.waterTankLevel = level;
        this.ts = System.currentTimeMillis();
    }

}
