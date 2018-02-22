package com.hashmapinc.tempus;

import java.io.Serializable;
import java.sql.Timestamp;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

public class TagDataCompressed implements Serializable{

  private static final long serialVersionUID = 4516441214893472229L;
  private long id;
  private byte[] vb;
  private byte[] q;
  private byte[] ts;
 
  private long ns;
  private Timestamp stTs;
  private Timestamp upTs;

  /**
   * Default Constructor
   */
  public TagDataCompressed() {
  }

  /**
   * @return the id
   */
  public long getId() {
    return id;
  }

  /**
   * @param uri the uri to set
   */
  public void setId(long l) {
    this.id = l;
  }

  

  /**
   * @return the vb
   */
  public byte[] getVb() {
    return vb;
  }

  /**
   * @param vb the vb to set
   */
  public void setVb(byte[] vb) {
    this.vb = vb;
  }

  /**
   * @return the q
   */
  public byte[] getQ() {
    return q;
  }

  /**
   * @param q the q to set
   */
  public void setQ(byte[] q) {
    this.q = q;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }

  public byte[] getTs() {
    return ts;
  }

  public void setTs(byte[] ts) {
    this.ts = ts;
  }

  /**
   * @return the ns
   */
  public long getNs() {
    return ns;
  }

  /**
   * @param ns the ns to set
   */
  public void setNs(long ns) {
    this.ns = ns;
  }

  /**
   * @return the fstTs
   */
  public Timestamp getStTs() {
    return stTs;
  }

  /**
   * @param fstTs the fstTs to set
   */
  public void setStTs(Timestamp fstTs) {
    this.stTs = fstTs;
  }

  /**
   * @return the upTs
   */
  public Timestamp getUpTs() {
    return upTs;
  }

  /**
   * @param upTs the upTs to set
   */
  public void setUpTs(Timestamp upTs) {
    this.upTs = upTs;
  }

}