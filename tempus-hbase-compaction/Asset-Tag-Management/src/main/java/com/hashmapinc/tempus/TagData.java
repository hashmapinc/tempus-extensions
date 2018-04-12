package com.hashmapinc.tempus;

import java.io.Serializable;
import java.sql.Timestamp;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

public class TagData implements Serializable{


  private static final long serialVersionUID = -8372604788246454837L;
  
  private long uri;
  private Timestamp ts;
  private String vs;
  private long vl;
  private double vd;
  private Short q;
  
  private Timestamp minTs;
  private Timestamp maxTs;
  
  /**
   * Default Constructor
   */
  public TagData() {
  }

  /**
   * @return the URI
   */
  public long getUri() {
    return uri;
  }

  /**
   * @param uri the URI to set
   */
  public void setUri(long uri) {
    this.uri = uri;
  }

  /**
   * @return the ts
   */
  public Timestamp getTs() {
    return ts;
  }

  /**
   * @param ts the ts to set
   */
  public void setTs(Timestamp ts) {
    this.ts = ts;
  }

  /**
   * @return the vs
   */
  public String getVs() {
    return vs;
  }

  /**
   * @param vs the vs to set
   */
  public void setVs(String vs) {
    this.vs = vs;
  }

  /**
   * @return the vl
   */
  public Long getVl() {
    return vl;
  }

  /**
   * @param vl the vl to set
   */
  public void setVl(long vl) {
    this.vl = vl;
  }

  /**
   * @return the vd
   */
  public double getVd() {
    return vd;
  }

  /**
   * @param vd the vd to set
   */
  public void setVd(double vd) {
    this.vd = vd;
  }
  
  /**
   * @return the q
   */
  public Short getQ() {
    return q;
  }

  /**
   * @param q the q to set
   */
  public void setQ(Short q) {
    this.q = q;
  }

  /**
   * @return the minTs
   */
  public Timestamp getMinTs() {
    return minTs;
  }

  /**
   * @param minTs the minTs to set
   */
  public void setMinTs(Timestamp minTs) {
    this.minTs = minTs;
  }

  /**
   * @return the maxTs
   */
  public Timestamp getMaxTs() {
    return maxTs;
  }

  /**
   * @param maxTs the maxTs to set
   */
  public void setMaxTs(Timestamp maxTs) {
    this.maxTs = maxTs;
  }
  
  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
