package com.hashmapinc.tempus.codec.impl;

import com.hashmapinc.tempus.TagData;
import com.hashmapinc.tempus.codec.ValueCodec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

public class ShortValueCodec extends ValueCodec {

    @Override
    public void packValue(TagData tagData, DataOutputStream dos) throws IOException {
        Short val = ((Long) tagData.getVl()).shortValue();
        dos.writeShort(val);
    }

    @Override
    public TagData unpackValue(
            DataInputStream dis) throws IOException {
        try {
            Short b = dis.readShort();
            TagData tagData = new TagData();
            tagData.setVl(b);
            return tagData;
        } catch (EOFException eofe) {
            //absorb the exception
            return null;
        }
    }

    public void packQuality(Short val, DataOutputStream dos) throws IOException {
        dos.writeShort(val);
    }

    public Short unPackQuality(DataInputStream dis) throws IOException {
        try {
            Short b = dis.readShort();
            return b;
        } catch (EOFException eofe) {
            //absorb the exception
            return null;
        }
    }

}
