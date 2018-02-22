package com.hashmapinc.tempus;

import java.sql.Timestamp;

public class CompactionStatus {

  private long numCompactedRecords;
  private Timestamp lastCompactionTs;

  public CompactionStatus(long numCompactedRecords) {
    // TODO Auto-generated constructor stub
    this.numCompactedRecords = numCompactedRecords;
  }

  /**
   * @return the numCompactedRecords
   */
  public long getNumCompactedRecords() {
    return numCompactedRecords;
  }

  /**
   * @param numCompactedRecords the numCompactedRecords to set
   */
  public void setNumCompactedRecords(long numCompactedRecords) {
    this.numCompactedRecords = numCompactedRecords;
  }

  /**
   * @return the lastCompactionTs
   */
  public Timestamp getLastCompactionTs() {
    return lastCompactionTs;
  }

  /**
   * @param lastCompactionTs the lastCompactionTs to set
   */
  public void setLastCompactionTs(Timestamp lastCompactionTs) {
    this.lastCompactionTs = lastCompactionTs;
  }

}
