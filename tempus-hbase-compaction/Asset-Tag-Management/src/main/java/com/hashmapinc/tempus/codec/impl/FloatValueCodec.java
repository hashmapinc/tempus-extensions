package com.hashmapinc.tempus.codec.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import com.hashmapinc.tempus.TagData;
import com.hashmapinc.tempus.codec.ValueCodec;

import org.apache.log4j.Logger;


public class FloatValueCodec extends ValueCodec {
	private static final Logger log = Logger.getLogger(FloatValueCodec.class);

	@Override
	public void packValue(TagData tagData, DataOutputStream dos) throws IOException {
		Float val = ((Double)tagData.getVd()).floatValue();
		dos.writeFloat(val);
	}

	@Override
	public TagData unpackValue(DataInputStream dis) throws IOException {
		try {
			Float b = dis.readFloat();
			TagData tagData = new TagData();
			tagData.setVd(b);
			return tagData;
		} catch (EOFException eofe) {
			return null;
		}
	}
	
}
