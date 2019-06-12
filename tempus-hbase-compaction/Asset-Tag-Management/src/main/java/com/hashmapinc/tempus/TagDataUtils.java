package com.hashmapinc.tempus;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.commons.lang.NullArgumentException;

import com.hashmapinc.tempus.codec.ValueCodec;
import com.hashmapinc.tempus.codec.impl.BooleanValueCodec;
import com.hashmapinc.tempus.codec.impl.ByteValueCodec;
import com.hashmapinc.tempus.codec.impl.DWordValueCodec;
import com.hashmapinc.tempus.codec.impl.FloatValueCodec;
import com.hashmapinc.tempus.codec.impl.LongValueCodec;
import com.hashmapinc.tempus.codec.impl.ShortValueCodec;
import com.hashmapinc.tempus.codec.impl.StringValueCodec;
import com.hashmapinc.tempus.codec.impl.WordValueCodec;

public class TagDataUtils {

  public static ValueCodec getCodec(String dataType) {
    ValueCodec vc = null;
    if (dataType == null){
      throw new NullArgumentException("dataType to fetch ValueCodec is null");
    }
    if (dataType.equals("String")) {
      vc = new StringValueCodec();
    } else if (dataType.equals("Float")) {
      vc = new FloatValueCodec();
    } else {

      if (dataType.equals("Boolean")) {
        vc = new BooleanValueCodec();

      } else if (dataType.equals("Byte")) {
        vc = new ByteValueCodec();

      } else if (dataType.equals("Short")) {
        vc = new ShortValueCodec();

      } else if (dataType.equals("Word")) {
        vc = new WordValueCodec();

      } else if (dataType.equals("DWord")) {
        vc = new DWordValueCodec();

      } else if (dataType.equals("Long")) {
        vc = new LongValueCodec();
      } else {
        // Don't know how to deal with types other than
        // the above yet
        throw new IllegalArgumentException("Unknown datatype for fetching ValueCodec");
      }
    }
    return vc;
  }
}
