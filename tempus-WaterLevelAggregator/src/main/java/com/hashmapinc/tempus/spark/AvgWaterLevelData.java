package com.hashmapinc.tempus.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by ashvayka on 15.03.17.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AvgWaterLevelData implements Serializable {

    private double value;
    private int count;

    public AvgWaterLevelData(double value) {
        this.value = value;
        this.count = 1;
    }

    public double getAvgValue() {
        return value / count;
    }

    public static AvgWaterLevelData sum(AvgWaterLevelData a, AvgWaterLevelData b) {
        return new AvgWaterLevelData(a.value + b.value, a.count + b.count);
    }

}
