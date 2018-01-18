package com.hashmapinc.tempus.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WindSpeedData implements Serializable {

    private String geoZone;
    private double windSpeed;

}
