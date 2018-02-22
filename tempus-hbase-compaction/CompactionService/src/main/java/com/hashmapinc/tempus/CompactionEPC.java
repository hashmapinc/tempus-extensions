package com.hashmapinc.tempus;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.hashmapinc.tempus.TagData;
import com.hashmapinc.tempus.TagDataCompressed;
import com.hashmapinc.tempus.CompactionProtos.CompactedData;
import com.hashmapinc.tempus.CompactionProtos.CompactionRequest;
import com.hashmapinc.tempus.CompactionProtos.CompactionResponse;
import com.hashmapinc.tempus.CompactionProtos.CompactionService;
import com.hashmapinc.tempus.CompactionProtos.CompactedData.Builder;
import com.hashmapinc.tempus.codec.ValueCodec;
import com.hashmapinc.tempus.codec.impl.LongValueCodec;
import com.hashmapinc.tempus.codec.impl.ShortValueCodec;

public class CompactionEPC extends CompactionService implements Coprocessor, CoprocessorService {

  private final static transient Logger log = Logger.getLogger(CompactionEPC.class);
  private final static int ROW_KEY_URI_LENGTH = 8;
  private final static int ROW_KEY_TS_LENGTH = 8;
  RegionCoprocessorEnvironment regionEnv = null;

  Scan scan = new Scan();
  byte[] familyBytes = Bytes.toBytes("0");
  String[] qualifiers = new String[] { "Q", "VS", "VL", "VD", "_0" };

  public CompactionEPC() {
    // TODO Auto-generated constructor stub
    log.debug("CompactionEPC Constructor");
    for (String qualifier : qualifiers) {
      byte[] qualifierBytes = Bytes.toBytes(qualifier);
      scan.addColumn(familyBytes, qualifierBytes);
    }
  }

  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      regionEnv = (RegionCoprocessorEnvironment) env;
      log.info("CompactionEPC loading Success");

    } else {
      throw new CoprocessorException("Must be loaded on a table region");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
  }

  private byte[] createScanRow(long uri, Long tsMillis, Integer tsNanos) {
    log.debug("uri[" + uri + "]; tsMillis[" + tsMillis + "]; tsNanos[" + tsNanos + "];");
    Long hbUri = -(Long.MIN_VALUE - uri);
    Long hbMillisTs = -(Long.MIN_VALUE - tsMillis);

    byte[] bytesURI = Bytes.toBytes(hbUri);
    byte[] bytesTsMillis = Bytes.toBytes(hbMillisTs);
    byte[] bytesTsNanos = null;

    byte[] hbRowKey = null;
    if (tsNanos != null) {
      Integer hbNanosTs = tsNanos;
      bytesTsNanos = Bytes.toBytes(hbNanosTs);
      hbRowKey = new byte[bytesURI.length + bytesTsMillis.length + bytesTsNanos.length];
    } else {
      hbRowKey = new byte[bytesURI.length + bytesTsMillis.length];
    }
    log.debug("hbRowKey.length " + hbRowKey.length);
    int incrOffset = Bytes.putBytes(hbRowKey, 0, bytesURI, 0, bytesURI.length);
    incrOffset = Bytes.putBytes(hbRowKey, incrOffset, bytesTsMillis, 0, bytesTsMillis.length);
    if (tsNanos != null) {
      incrOffset = Bytes.putBytes(hbRowKey, incrOffset, bytesTsNanos, 0, bytesTsNanos.length);
    }
    return hbRowKey;
  }

  private byte[] createScanStartRow(CompactionRequest request) {
    long startURI = request.getUri();
    Long origStartTs = request.getStartTime();
    log.debug("startURI[" + startURI + "]; startTs[" + origStartTs + "];");

    byte[] startRow = null;
    if (origStartTs == 0) {
      Long hbUri = -(Long.MIN_VALUE - startURI);
      byte[] bytesStartURI = Bytes.toBytes(hbUri);
      startRow = new byte[bytesStartURI.length];
      Bytes.putBytes(startRow, 0, bytesStartURI, 0, bytesStartURI.length);
    } else {
      return createScanRow(request.getUri(), request.getStartTime(), null);
    }
    return startRow;
  }

  private byte[] createScanStopRow(CompactionRequest request) {
    return createScanRow(request.getUri(), request.getEndTime(), null);
  }

  private CompactionResponse.Builder scanTagData(Scan scan, CompactionRequest request,
      RpcController controller) {

    long reqURI = request.getUri();

    CompactionResponse.Builder responseBuilder = CompactionResponse.newBuilder();
    InternalScanner scanner = null;
    try {
      scanner = regionEnv.getRegion().getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      boolean hasMore = false;

      List<TagData> tagDataList = new ArrayList<TagData>();

      ByteArrayOutputStream baosTs = new ByteArrayOutputStream();
      DataOutputStream dosTs = new DataOutputStream(baosTs);
      LongValueCodec lvc = new LongValueCodec();

      ByteArrayOutputStream baosQ = new ByteArrayOutputStream();
      DataOutputStream dosQ = new DataOutputStream(baosQ);
      ShortValueCodec qvc = new ShortValueCodec();

      TagDataCompressed tdc = new TagDataCompressed();

      Builder compactedData = CompactedData.newBuilder();

      int count = 0;
      do {
        TagData tagData = new TagData();

        hasMore = scanner.next(results);
        int resultsSize = results.size();

        log.info("No of cells:- " + resultsSize);
        Boolean isScanUriEqualsRequestUri = false;
        Short quality = null;
        String vs = null;
        Long vl = null;
        Double vd = null;
        Long uri = null;
        Long tsMillis = null;
        for (Cell cell : results) {

          byte[] rowKey = CellUtil.cloneRow(cell);

          byte[] uriBytes = Arrays.copyOfRange(rowKey, 0, ROW_KEY_URI_LENGTH);
          byte[] tsMillisBytes = Arrays.copyOfRange(rowKey, ROW_KEY_URI_LENGTH, ROW_KEY_URI_LENGTH + ROW_KEY_TS_LENGTH);

          log.debug("rowKey[" + rowKey + "];uriBytes[" + uriBytes + "]; tsMillisBytes["
              + tsMillisBytes + "];");

          uri = -(Long.MIN_VALUE - (long) Bytes.toLong(uriBytes, 0, uriBytes.length));
          Long posUri = -(Long.MIN_VALUE - (long) Bytes.toLong(uriBytes, 0, uriBytes.length));
          Long negUri = (long) Bytes.toLong(uriBytes, 0, uriBytes.length);
          log.debug("posUri[" + posUri + "];negUri[" + negUri + "];");

          log.debug("uri[" + uri + "];reqURI[" + reqURI + "];");
          if (reqURI == uri) {
            count += 1;
            log.debug("reqURI == uri " + reqURI + ":" + uri);
            // All the cells will be for a particular point tag. our Scan ensures that
            // For all the records fetched we only set the record @ first instance
            if (count == 1) {
              long firstTsMs =
                  -(Long.MIN_VALUE - (long) Bytes.toLong(tsMillisBytes, 0, tsMillisBytes.length));
              log.debug("firstTsMs[" + firstTsMs + "]");

              compactedData.setUri(uri);
              compactedData.setFirstptTs(firstTsMs);
            }
            isScanUriEqualsRequestUri = true;
          } else {
            log.debug("reqURI != uri " + reqURI + ":" + uri);
            isScanUriEqualsRequestUri = false;
            break;
          }

          byte[] hexValue = CellUtil.cloneValue(cell);
          String qualifierStr = Bytes.toString(CellUtil.cloneQualifier(cell));
          log.info(String.format("qualifierStr [%s]: ", qualifierStr));
          if (qualifierStr.equals("Q")) {
            quality = (short) -(Short.MIN_VALUE - Bytes.toShort(hexValue, 0, hexValue.length));
            qvc.packQuality(quality, dosQ);
          } else if (qualifierStr.equals("VS")) {
            vs = Bytes.toString(hexValue);
            tagData.setVs(vs);
          } else if (qualifierStr.equals("VL")) {
            vl = -(Long.MIN_VALUE - (long) Bytes.toLong(hexValue, 0, hexValue.length));
            tagData.setVl(vl);
          } else if (qualifierStr.equals("VD")) {
            vd = -Bytes.toDouble(hexValue);
            tagData.setVd(vd);
          } else if (qualifierStr.equals("_0")) {
            log.info("timestamp is:- " + cell.getTimestamp());
            tsMillis = //cell.getTimestamp();
                -(Long.MIN_VALUE - (long) Bytes.toLong(tsMillisBytes, 0, tsMillisBytes.length));
            lvc.packValue(tsMillis, dosTs);
          }
        }
        log.info(
          String.format("For uri[%d]:- ts[%d]; vl[%d]; vd[%f]; vs[%s]; q[%d];", uri, tsMillis, vl, vd, vs, quality));

        if (isScanUriEqualsRequestUri) {
          tagDataList.add(tagData);
        }
        results.clear();
      } while (hasMore);

      if (tagDataList.size() > 0) {
        if (compactedData.getUri() == -1) {
          responseBuilder.setIsFail(true).setErrMsg("ERR: URI is -1.");
        } else {
          String uriDataType = request.getDataType();
          ValueCodec vc = TagDataUtils.getCodec(uriDataType);
          if (vc == null) {
            responseBuilder.setIsFail(true).setErrMsg("ERR: Unsupported type : " + uriDataType
                + " for URI " + reqURI + ". Cannot compact the data");
          } else {
            final ByteArrayOutputStream baos = vc.compress(tagDataList);
            tdc.setTs(baosTs.toByteArray());
            tdc.setVb(baos.toByteArray());
            tdc.setQ(baosQ.toByteArray());

            compactedData.setTs(ByteString.copyFrom(tdc.getTs()));
            compactedData.setVb(ByteString.copyFrom(tdc.getVb()));
            compactedData.setQuality(ByteString.copyFrom(tdc.getQ()));
            compactedData.setNumSamples(tagDataList.size());

            responseBuilder.setPackedData(compactedData.build()).setIsFail(false);
          }
        }
      } else {
        responseBuilder.setIsFail(false).setErrMsg("NO_ERR: No cells present for uri " + reqURI);
      }
    } catch (IOException ioe) {
      log.error("Recvd IOException exception:- " + ioe.getMessage());
      ResponseConverter.setControllerException(controller, ioe);
      responseBuilder.setIsFail(true).setErrMsg(ioe.getMessage());
    } catch (Exception exc) {
      //log.error("Recvd exception:- " + exc.getMessage());
      responseBuilder.setIsFail(true).setErrMsg("Unknown exception caught");
      exc.printStackTrace();
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ioe) {
          log.error("Recvd IOException exception2:- " + ioe.getMessage());
          ResponseConverter.setControllerException(controller, ioe);
          responseBuilder.setIsFail(true).setErrMsg(ioe.getMessage());
        }
      }
    }
    return responseBuilder;

  }

  @Override
  public void compactData(RpcController controller, CompactionRequest request,
      RpcCallback<CompactionResponse> done) {

    log.debug("Compaction Request for URI[" + request.getUri() + "]@[" + request.getStartTime()
        + "-" + request.getEndTime() + "]");

    byte[] startRow = createScanStartRow(request);
    byte[] stopRow = createScanStopRow(request);

    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);

    CompactionResponse response = scanTagData(scan, request, controller).build();
    log.debug(
      "response:- " + response.toString() + "; response size:- " + response.getSerializedSize());
    done.run(response);
  }
}