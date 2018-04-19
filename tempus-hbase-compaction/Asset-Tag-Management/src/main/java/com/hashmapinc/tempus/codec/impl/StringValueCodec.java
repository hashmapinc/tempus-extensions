package com.hashmapinc.tempus.codec.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import com.hashmapinc.tempus.TagData;
import com.hashmapinc.tempus.codec.ValueCodec;

import org.apache.log4j.Logger;


public class StringValueCodec extends ValueCodec {
	private static final Logger log = Logger.getLogger(StringValueCodec.class);
	@Override
	public void packValue(TagData tagData, DataOutputStream dos) throws IOException {
		String s = (tagData.getVs() == null) ? "" : tagData.getVs();
		short length = (short)s.length();
		dos.writeShort(length);
		dos.writeBytes(s);
	}

	@Override
	public TagData unpackValue(
			DataInputStream dis) throws IOException {
		try {
			Short length = dis.readShort();
			byte [] b = new byte[length];
			dis.read(b);
			TagData tagData = new TagData();
			tagData.setVs(new String(b));
			return tagData;
		} catch (EOFException eofe) {
			//absorb the exception
			return null;
		}
	}
	
}
