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

  /**
   * Returns a comparator for TagData ts
   * @return
   */
  public static Comparator<TagData> tagDataTsComparator() {
    return ((Comparator<TagData> & Serializable) (TagData i1, TagData i2) -> {
      if (i1 == null) {
        if (i2 == null) {
          return 0;
        } else {
          return -1;
        }
      } else if (i2 == null) {
        return 1;
      }
      if (i1.getTs().before(i2.getTs())) {
        return -1;
      } else if (i1.getTs().after(i2.getTs())) {
        return 1;
      }
      return 0;
    });
  }

  /**
   * Returns a comparator for TagData by uri, then ts
   * @return
   */
  public static Comparator<TagData> tagDataUriTsComparator() {
    return ((Comparator<TagData> & Serializable) (TagData i1, TagData i2) -> {
      if (i1 == null) {
        if (i2 == null) {
          return 0;
        } else {
          return -1;
        }
      } else if (i2 == null) {
        return 1;
      }

    /*  int res = String.CASE_INSENSITIVE_ORDER.compare(i1.getPointTag(), i2.getPointTag());
      if (res == 0) {
        return 1;
      }
*/
      if (i1.getTs().before(i2.getTs())) {
        return -1;
      } else if (i1.getTs().after(i2.getTs())) {
        return 1;
      }
      return 0;
    });
  }

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
