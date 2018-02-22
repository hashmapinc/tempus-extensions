package com.hashmapinc.tempus.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hashmapinc.tempus.TagData;

public abstract class ValueCodec implements TagValueCodec {
  protected ByteArrayOutputStream baos;
  protected DataOutputStream dos;

  protected ByteArrayInputStream bais;
  protected DataInputStream dis;

  public ValueCodec() {
    baos = new ByteArrayOutputStream();
    dos = new DataOutputStream(baos);
  }

  @Override
  public ByteArrayOutputStream compress(List<TagData> tagDataList) throws IOException {
    baos.reset();

    for (TagData tagData : tagDataList) {
      packValue(tagData, dos);
    }
    dos.flush();
    return baos;
  }

  @Override
  public ByteArrayOutputStream compress(TagData tagData) throws IOException {
    baos.reset();
    packValue(tagData, dos);
    dos.flush();
    return baos;
  }

  @Override
  public List<TagData> decompress(byte[] compressedData) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<TagData> decompress(ByteArrayInputStream compressedData) throws IOException {
    List<TagData> tagDataList = new ArrayList<TagData>();
    dis = new DataInputStream(compressedData);
    TagData tagData;

    while ((tagData = unpackValue(dis)) != null) {
      tagDataList.add(tagData);
    }

    return tagDataList;
  }

  public abstract void packValue(TagData tagData, DataOutputStream dos) throws IOException;

  public abstract TagData unpackValue(DataInputStream dis) throws IOException;
}
