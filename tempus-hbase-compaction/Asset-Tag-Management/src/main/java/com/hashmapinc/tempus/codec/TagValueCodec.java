package com.hashmapinc.tempus.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.hashmapinc.tempus.TagData;

/**
 * @author vmthattikota Compressor interface that compresses hourly worth of tag values into one
 *         blob for optimized storage
 */
public interface TagValueCodec {

  /**
   * @param tagDataList
   * @return
   */
  ByteArrayOutputStream compress(final List<TagData> tagDataList) throws IOException;

  ByteArrayOutputStream compress(final TagData tagData) throws IOException;

  /**
   * @param compressedData
   * @return
   */
  //List<TagData> decompress(byte[] compressedData);

  /**
   * @param compressedData
   * @return
   */
  List<TagData> decompress(ByteArrayInputStream compressedData) throws IOException;
}
