package com.hashmapinc.tempus;

import com.hashmapinc.tempus.CompactionProtos.CompactionRequest;

public class RpcCalls {

  private byte[] startKey;
  private byte[] endKey;
  private CompactionRequest request;


  public RpcCalls(final byte[] startKey, final byte[] endKey, final CompactionRequest request) {
    this.startKey = startKey;
    this.endKey = endKey;
    this.request = request;
  }

  public byte[] getStartKey() {
    return startKey;
  }

  public byte[] getEndKey() {
    return endKey;
  }

  public CompactionRequest getRequest() {
    return request;
  }
}