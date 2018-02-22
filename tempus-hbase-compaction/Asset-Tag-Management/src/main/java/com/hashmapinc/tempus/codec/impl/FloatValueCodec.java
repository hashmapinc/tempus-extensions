package com.hashmapinc.tempus.codec.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import com.hashmapinc.tempus.TagData;
import com.hashmapinc.tempus.codec.ValueCodec;


public class FloatValueCodec extends ValueCodec {

	@Override
	public void packValue(TagData tagData, DataOutputStream dos) throws IOException {
		Float val = ((Double)tagData.getVd()).floatValue();
		dos.writeFloat(val);
	}
	
	public TagData unpackValue(
			DataInputStream dis) throws IOException {
	  try {
			Float b = dis.readFloat();
			TagData tagData = new TagData();
			tagData.setVd(b);
			return tagData;
		} catch (EOFException eofe) {
			//absorb the exception
			return null;
		}
	}
	
}
