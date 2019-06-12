package com.hashmapinc.tempus;

import java.io.Serializable;

public class TagList implements Serializable {

  //This is the URI field in TagData
  private long id;    
  private String channelUri;  
  private String assetid;       
  private String channelName;   
  private String source;       
  private String channelId;
  private Short status;
  private String dataType;
  
  public TagList() {}

  /**
   * @return the id
   */
  public long getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(long id) {
    this.id = id;
  }

  /**
   * @return the channelUri
   */
  public String getChannelUri() {
    return channelUri;
  }

  /**
   * @param channelUri the channelUri to set
   */
  public void setChannelUri(String channelUri) {
    this.channelUri = channelUri;
  }

  /**
   * @return the assetid
   */
  public String getAssetid() {
    return assetid;
  }

  /**
   * @param assetid the assetid to set
   */
  public void setAssetid(String assetid) {
    this.assetid = assetid;
  }

  /**
   * @return the channelName
   */
  public String getChannelName() {
    return channelName;
  }

  /**
   * @param channelName the channelName to set
   */
  public void setChannelName(String channelName) {
    this.channelName = channelName;
  }

  /**
   * @return the source
   */
  public String getSource() {
    return source;
  }

  /**
   * @param source the source to set
   */
  public void setSource(String source) {
    this.source = source;
  }

  /**
   * @return the channelId
   */
  public String getChannelId() {
    return channelId;
  }

  /**
   * @param channelId the channelId to set
   */
  public void setChannelId(String channelId) {
    this.channelId = channelId;
  }

  /**
   * @return the status
   */
  public Short getStatus() {
    return status;
  }

  /**
   * @param status the status to set
   */
  public void setStatus(Short status) {
    this.status = status;
  }

  /**
   * @return the dataType
   */
  public String getDataType() {
    return dataType;
  }

  /**
   * @param dataType the dataType to set
   */
  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "TagList [id=" + id + ", channelUri=" + channelUri + ", assetid=" + assetid
        + ", channelName=" + channelName + ", source=" + source + ", channelId=" + channelId
        + ", status=" + status + ", dataType=" + dataType + "]";
  }
}
