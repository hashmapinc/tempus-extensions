package com.hashmapinc.tempus.codec.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import com.hashmapinc.tempus.TagData;
import com.hashmapinc.tempus.codec.ValueCodec;

public class ByteValueCodec extends ValueCodec {

  @Override
  public void packValue(TagData tagData, DataOutputStream dos) throws IOException {
    Short val = ((Long) tagData.getVl()).shortValue();
    dos.writeShort(val);
  }

  public TagData unpackValue(DataInputStream dis) throws IOException {
    try {
      Short val = dis.readShort();
      TagData tagData = new TagData();
      tagData.setVl(val);
      return tagData;
    } catch (EOFException eofe) {
      // absorb the exception
      return null;
    }
  }

}
