package com.hashmapinc.tempus.codec.impl;

import com.hashmapinc.tempus.TagData;
import com.hashmapinc.tempus.codec.ValueCodec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;


public class LongValueCodec extends ValueCodec {

    @Override
    public void packValue(TagData tagData, DataOutputStream dos) throws IOException {
        dos.writeLong(tagData.getVl());
    }

    @Override
    public TagData unpackValue(
            DataInputStream dis) throws IOException {
        try {
            Long b = dis.readLong();
            TagData tagData = new TagData();
            tagData.setVl(b);
            return tagData;
        } catch (EOFException eofe) {
            //absorb the exception
            return null;
        }

    }

    public void packTs(Long vl, DataOutputStream dos) throws IOException {
        dos.writeLong(vl);
    }

    public Long unPackTs(DataInputStream dis) throws IOException {
        try {
            Long b = dis.readLong();
            return b;
        } catch (EOFException eofe) {
            //absorb the exception
            return null;
        }
    }


}
