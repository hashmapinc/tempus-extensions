package com.hashmapinc.tempus.codec.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import com.hashmapinc.tempus.TagData;
import com.hashmapinc.tempus.codec.ValueCodec;

public class WordValueCodec extends ValueCodec {

	@Override
	public void packValue(TagData tagData, DataOutputStream dos) throws IOException {
		Integer val = ((Long)tagData.getVl()).intValue();
		dos.writeInt(val);
	}
	
	public TagData unpackValue(
			DataInputStream dis) throws IOException {
	  try {
			Integer val = dis.readInt();
			TagData tagData = new TagData();
			tagData.setVl(val);
			return tagData;
		} catch (EOFException eofe) {
			//absorb the exception
			return null;
		}
	}	
}
