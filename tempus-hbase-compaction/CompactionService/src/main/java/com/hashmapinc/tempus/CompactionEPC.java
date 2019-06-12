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
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PVarchar;

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
  private final static int ROW_KEY_URI_LENGTH = PDataType.fromSqlTypeName("BIGINT").getByteSize();
  private final static int ROW_KEY_TS_LENGTH = PDataType.fromSqlTypeName("DATE").getByteSize();
  private static Boolean doLogging = false;
  RegionCoprocessorEnvironment regionEnv = null;

  Scan scan = new Scan();
  byte[] familyBytes = Bytes.toBytes("0");
  String[] qualifiers = new String[] { "Q", "VS", "VL", "VD", "_0" };

  public CompactionEPC() {
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
      log.info("ROW_KEY_URI_LENGTH[" + ROW_KEY_URI_LENGTH + "];ROW_KEY_TS_LENGTH[" + ROW_KEY_TS_LENGTH + "];");

    } else {
      throw new CoprocessorException("Must be loaded on a table region");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
  }

  private CompactionResponse.Builder scanTagData(Scan scan, ValueCodec vc, CompactionRequest
          request, RpcController controller) {

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
        if(resultsSize == 0)
          break;
        if(doLogging)
          log.info("resultsSize: " + resultsSize);
        Long vl = null;
        Double vd = null;
        String vs = null;
        Long tsMillis = null;
        Short quality = null;

        for (Cell cell : results) {
          byte[] rowKey = CellUtil.cloneRow(cell);
          byte[] uriBytes = Arrays.copyOfRange(rowKey, 0, ROW_KEY_URI_LENGTH);
          byte[] tsMillisBytes = Arrays.copyOfRange(rowKey, ROW_KEY_URI_LENGTH, ROW_KEY_URI_LENGTH + ROW_KEY_TS_LENGTH);
          Long uri = (Long) PLong.INSTANCE.toObject(uriBytes);
          // All the cells will be for a particular point tag. our Scan ensures that
          if (count == 0) {
            Long firstTs = (Long) PLong.INSTANCE.toObject(tsMillisBytes);
            if(doLogging)
              log.info("uri[" + uri + "];reqURI[" + reqURI + "];firstTs[" + firstTs + "];");
            compactedData.setUri(uri);
            compactedData.setFirstptTs(firstTs);
            count += 1;
          }
          byte[] hexValue = CellUtil.cloneValue(cell);
          String qualifierStr = Bytes.toString(CellUtil.cloneQualifier(cell));
          log.debug(String.format("qualifierStr [%s]: ", qualifierStr));
          switch(qualifierStr){
            case "VL":{
              vl = (Long) PLong.INSTANCE.toObject(hexValue);
              tagData.setVl(vl);
              break;
            }
            case "VS": {
              vs = (String) PVarchar.INSTANCE.toObject(hexValue);
              tagData.setVs(vs);
              break;
            }
            case "VD": {
              vd = (Double) PDouble.INSTANCE.toObject(hexValue);
              tagData.setVd(vd);
              break;
            }
            case "_0":{
              tsMillis = (Long) PLong.INSTANCE.toObject(tsMillisBytes);
              lvc.packTs(tsMillis, dosTs);
              break;
            }
            case "Q":{
              quality = (Short) PSmallint.INSTANCE.toObject(hexValue);
              qvc.packQuality(quality, dosQ);
              break;
            }
            default:
              break;
          }
        }
        if(doLogging)
          log.info(String.format("For uri[%d]; ts[%d]; vl[%d]; vd[%f]; vs[%s]; q[%d];",
                  compactedData.getUri(), tsMillis, tagData.getVl(), tagData.getVd(), tagData
                          .getVs(), quality));

        tagDataList.add(tagData);
        results.clear();
      } while (hasMore);

      if (tagDataList.size() > 0) {
        final ByteArrayOutputStream baos = vc.compress(tagDataList);
        tdc.setVb(baos.toByteArray());
        tdc.setTs(baosTs.toByteArray());
        tdc.setQ(baosQ.toByteArray());
        compactedData.setTs(ByteString.copyFrom(tdc.getTs()));
        compactedData.setVb(ByteString.copyFrom(tdc.getVb()));
        compactedData.setQuality(ByteString.copyFrom(tdc.getQ()));
        compactedData.setNumSamples(tagDataList.size());
        responseBuilder.setPackedData(compactedData.build()).setIsFail(false);
      } else {
        responseBuilder.setIsFail(false).setErrMsg("NO_ERR: No cells present for uri " + reqURI);
      }
    } catch (IOException ioe) {
      log.error("Recvd IOException exception:- " + ioe.getMessage());
      ResponseConverter.setControllerException(controller, ioe);
      responseBuilder.setIsFail(true).setErrMsg(ioe.getMessage());
    } catch (Exception exc) {
      log.error("Recvd exception:- " + exc.getMessage());
      responseBuilder.setIsFail(true).setErrMsg(exc.getMessage());
      exc.printStackTrace();
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ioe) {
          log.error("Recvd IOException exception when closing scanner:- " + ioe.getMessage());
          ResponseConverter.setControllerException(controller, ioe);
          responseBuilder.setIsFail(true).setErrMsg("Exception in closing scanner: " + ioe.getMessage());
        }
      }
    }
    return responseBuilder;

  }

  @Override
  public void compactData(RpcController controller, CompactionRequest request,
      RpcCallback<CompactionResponse> done) {

    if(System.getenv("DBG_COMPACTION_LOGS") != null){
      doLogging = true;
    }

    if(doLogging)
      log.info("Compaction Request for URI[" + request.getUri() + "]@[" + request.getStartTime()
            + "-" + request.getEndTime() + "]");
    CompactionResponse response = null;

    String uriDataType = request.getDataType();
    ValueCodec vc = TagDataUtils.getCodec(uriDataType);
    if (vc == null) {
      CompactionResponse.Builder responseBuilder = CompactionResponse.newBuilder();
      responseBuilder.setIsFail(true).setErrMsg("ERR: Unsupported type : " + uriDataType
              + " for URI " + request.getUri() + ". Cannot compact the data");
      response = responseBuilder.build();
    }else{
      byte[] uriBytes = PDataType.fromSqlTypeName("BIGINT").toBytes(request.getUri());
      byte[] startTsBytes = PLong.INSTANCE.toBytes(request.getStartTime());
      byte[] endTsBytes = PLong.INSTANCE.toBytes(request.getEndTime());
      final byte[] startRow = Bytes.add(uriBytes, startTsBytes);
      final byte[] stopRow = Bytes.add(uriBytes, endTsBytes);
      scan.setStartRow(startRow);
      scan.setStopRow(stopRow);
      response = scanTagData(scan, vc, request, controller).build();
    }
    if(doLogging)
      log.debug("response:- " + response.toString() + "; response size:- " + response.getSerializedSize());
    done.run(response);
  }
}