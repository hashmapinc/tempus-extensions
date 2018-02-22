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
  public ByteArrayOutputStream compress(final List<TagData> tagDataList) throws IOException;

  public ByteArrayOutputStream compress(final TagData tagData) throws IOException;

  /**
   * @param compressedData
   * @return
   */
  public List<TagData> decompress(byte[] compressedData);

  /**
   * @param compressedData
   * @return
   */
  public List<TagData> decompress(ByteArrayInputStream compressedData) throws IOException;
}
