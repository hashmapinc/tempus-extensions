package com.hashmapinc.tempus.codec.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import com.hashmapinc.tempus.TagData;
import com.hashmapinc.tempus.codec.ValueCodec;


public class DWordValueCodec extends ValueCodec {

	@Override
	public void packValue(TagData tagData, DataOutputStream dos) throws IOException {
		Integer val = ((Long)tagData.getVl()).intValue();
		dos.writeInt(val);
	}

	@Override
	public TagData unpackValue(
			DataInputStream dis) throws IOException {
	  try {
			Integer b = dis.readInt();
			TagData tagData = new TagData();
			tagData.setVl(b);
			return tagData;
		} catch (EOFException eofe) {
			//absorb the exception
			return null;
		}
	}

}
