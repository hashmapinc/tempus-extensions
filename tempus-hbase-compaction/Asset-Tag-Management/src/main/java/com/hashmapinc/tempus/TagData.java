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
   * @return the pointTag
   */
  public long getUri() {
    return uri;
  }

  /**
   * @param pointTag the pointTag to set
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

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   *
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((maxTs == null) ? 0 : maxTs.hashCode());
    result = prime * result + ((minTs == null) ? 0 : minTs.hashCode());
    result = prime * result + ((pointTag == null) ? 0 : pointTag.hashCode());
    result = prime * result + ((q == null) ? 0 : q.hashCode());
    result = prime * result + ((ts == null) ? 0 : ts.hashCode());
    result = prime * result + ((vs == null) ? 0 : vs.hashCode());
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  
  /*
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    TagData other = (TagData) obj;
    if (maxTs == null) {
      if (other.maxTs != null) return false;
    } else if (!maxTs.equals(other.maxTs)) return false;
    if (minTs == null) {
      if (other.minTs != null) return false;
    } else if (!minTs.equals(other.minTs)) return false;
    if (pointTag == null) {
      if (other.pointTag != null) return false;
    } else if (!pointTag.equals(other.pointTag)) return false;
    if (q == null) {
      if (other.q != null) return false;
    } else if (!q.equals(other.q)) return false;
    if (ts == null) {
      if (other.ts != null) return false;
    } else if (!ts.equals(other.ts)) return false;
    if (vs == null) {
      if (other.vs != null) return false;
    } else if (!vs.equals(other.vs)) return false;
    return true;
  }*/
}
