package com.hashmapinc.tempus.codec.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import com.hashmapinc.tempus.TagData;
import com.hashmapinc.tempus.codec.ValueCodec;

public class BooleanValueCodec extends ValueCodec {

  @Override
  public void packValue(TagData tagData, DataOutputStream dos) throws IOException {
    Boolean val = tagData.getVl() == 0 ? false : true;
    dos.writeBoolean(val);
  }

  public void packQuality(TagData tagData, DataOutputStream dos) throws IOException {
    //TODO
    Boolean val = true;//tagData.getQ();
    dos.writeBoolean(val);
  }

  public void packQuality(Boolean val, DataOutputStream dos) throws IOException {
    dos.writeBoolean(val);
  }

  public TagData unpackQuality(DataInputStream dis) throws IOException {
    try {
      Boolean val = dis.readBoolean();
      TagData tagData = new TagData();
      //TODO
      //tagData.setQ(val);
      return tagData;
    } catch (EOFException eofe) {
      return null;
    }
  }

  public TagData unpackValue(DataInputStream dis) throws IOException {
    try {
      Boolean b = dis.readBoolean();
      TagData tagData = new TagData();
      tagData.setVl(b == true ? 1 : 0);
      return tagData;
    } catch (EOFException eofe) {
      // absorb the exception
      return null;
    }
  }
}
