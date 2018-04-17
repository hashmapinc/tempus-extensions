package com.hashmapinc.tempus;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.log4j.Logger;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;

import com.hashmapinc.tempus.codec.ValueCodec;
import com.hashmapinc.tempus.codec.impl.LongValueCodec;
import com.hashmapinc.tempus.codec.impl.ShortValueCodec;

// Eg Query:-
/*
 * select UNCOMPACT("VB", "Q", "TS", "NS", 'T1', 'T2', "ID")from td_compact where id = X and FIRSTTS
 * <=T2 and FIRSTTS >=T1.
 */
/*
 * We need to pass the foll parameters UNCOMPACT(VARBINARY, VARBINARY, VARBINARY, INTEGER, BIGINT,
 * BIGINT) Usage:- UNCOMPACT("VB", "Q", "TS", "NUMSAMPLES", FROMTIME, ENDTIME, "PT")
 * @param VARBINARY:- VB
 * @param VARBINARY:- Q
 * @param VARBINARY:- TS
 * @param INTEGER:- NS
 * @param VARCHAR:- Start TS - from time where we need the records
 * @param VARCHAR:- End TS - time till which we need records
 * @param VARCHAR:- PT - used to upsert the values to tduc (tag_data_uncompact) table
 */
@FunctionParseNode.BuiltInFunction(name = UnCompact.NAME,
    args = { @FunctionParseNode.Argument(allowedTypes = { PVarbinary.class }),
        @FunctionParseNode.Argument(allowedTypes = { PVarbinary.class }),
        @FunctionParseNode.Argument(allowedTypes = { PVarbinary.class }),
        @FunctionParseNode.Argument(allowedTypes = { PInteger.class }),
        @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }),
        @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }),
        @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }) })
public class UnCompact extends ScalarFunction {

  public static final String NAME = "UNCOMPACT";
  private ValueCodec valueCodec = null;
  private int estimatedByteSize = 0;
  static Logger log = Logger.getLogger(UnCompact.class);
  private static final String UNCOMPACTED_TABLE = "tduc";

  DatabaseService dbService = null;

  public UnCompact() {
  }

  public UnCompact(List<Expression> children) throws SQLException, ConfigurationException {
    super(children);
    dbService = new DatabaseService();
    try {
      dbService.openConnection();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new SQLException("dbService.openConnection() Exception");
    }
    String tagListTable = System.getenv("UNCOMPACT_TL_TABLE");
    if ((null == tagListTable) || (0 == tagListTable.length())) {
      throw new ConfigurationException(
          "Please set UNCOMPACT_TL_TABLE environment variable with value as tag_list table to find datatype of a URI.");
    }
    dbService.setTagListTable(tagListTable);
    // Drop Table if exists and Create again so that on every UDF execution we have data in it
    dbService.dropTable(UNCOMPACTED_TABLE);
    //
    dbService.createTable(UNCOMPACTED_TABLE);
  }

  private Expression getExpression(int index) {
    return children.get(index);
  }

  private byte[] getBinaryData(Expression binaryExpr, Tuple tuple, ImmutableBytesWritable ptr) {
    estimatedByteSize = PVarbinary.INSTANCE.estimateByteSize(
      (byte[]) binaryExpr.getDataType().toObject(ptr, binaryExpr.getSortOrder()));
    byte[] binaryValue = (byte[]) binaryExpr.getDataType().toObject(ptr, binaryExpr.getSortOrder(),
      estimatedByteSize, 0);
    // log.debug("estimatedByteSize = " + estimatedByteSize);
    // log.debug("binaryValue.length = " + binaryValue.length);

    return binaryValue;
  }

  @Override
  public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

    Expression binryExpr = getExpression(0);
    if (!binryExpr.evaluate(tuple, ptr)) {
      return false;
    } else if (ptr.getLength() == 0) {
      return true;
    }
    byte[] binaryValue = getBinaryData(binryExpr, tuple, ptr);
    if (binaryValue == null) {
      throw new RuntimeException("Error extracting Binary value for VS");
    }

    binryExpr = getExpression(1);
    if (!binryExpr.evaluate(tuple, ptr)) {
      return false;
    } else if (ptr.getLength() == 0) {
      return true;
    }
    byte[] binaryQ = getBinaryData(binryExpr, tuple, ptr);
    if (binaryQ == null) {
      throw new RuntimeException("Error extracting Binary value for Q");
    }

    binryExpr = getExpression(2);
    if (!binryExpr.evaluate(tuple, ptr)) {
      return false;
    } else if (ptr.getLength() == 0) {
      return true;
    }
    byte[] binaryTS = getBinaryData(binryExpr, tuple, ptr);
    if (binaryTS == null) {
      throw new RuntimeException("Error extracting Binary value for TS");
    }

    Expression numSamplesExpr = getExpression(3);
    if (!numSamplesExpr.evaluate(tuple, ptr)) {
      return false;
    } else if (ptr.getLength() == 0) {
      return true;
    }
    int numSamples =
        numSamplesExpr.getDataType().getCodec().decodeInt(ptr, numSamplesExpr.getSortOrder());
    if (numSamples <= 0) {
      log.debug("numSamples = " + numSamples);
      return false;
    }

    Expression startTsExpr = getExpression(4);
    if (!startTsExpr.evaluate(tuple, ptr)) {
      return false;
    } else if (ptr.getLength() == 0) {
      return true;
    }
    String startTsStr =
        (String) startTsExpr.getDataType().toObject(ptr, startTsExpr.getSortOrder());

    Expression stopTsExpr = getExpression(5);
    if (!stopTsExpr.evaluate(tuple, ptr)) {
      return false;
    } else if (ptr.getLength() == 0) {
      return true;
    }
    String stopTsStr = (String) stopTsExpr.getDataType().toObject(ptr, stopTsExpr.getSortOrder());

    if ((startTsStr == null) || (stopTsStr == null) || (startTsStr.length() == 0)
        || (stopTsStr.length() == 0)) {
      throw new RuntimeException("Start and Stop Timestamps can't be null or empty");
    }

    Timestamp startTs = Timestamp.valueOf(startTsStr);
    Timestamp stopTs = Timestamp.valueOf(stopTsStr);

    if (startTs.after(stopTs)) {
      throw new RuntimeException("Start TS can't occur after Stop TS");

    }
    log.debug(String.format("%s, %s", startTs.toString(), stopTs.toString()));
    log.debug(String.format("TS in long[%d, %d]", startTs.getTime(), stopTs.getTime()));

    Expression uriExpr = getExpression(6);
    if (!uriExpr.evaluate(tuple, ptr)) {
      return false;
    } else if (ptr.getLength() == 0) {
      return true;
    }
    Long uri = (Long) uriExpr.getDataType().toObject(ptr, uriExpr.getSortOrder());

    log.debug("Tuple size: " + tuple.size());
    return varBinaryToArray(ptr, binaryValue, binaryQ, binaryTS, startTs.getTime(),
      stopTs.getTime(), numSamples, uri, getSortOrder());
  }

  public List<Short> deCompactQ(ByteArrayInputStream compressedData) throws IOException {
    List<Short> unpackedQList = new ArrayList<Short>();
    DataInputStream dis = new DataInputStream(compressedData);
    Short unpackedQ;
    ShortValueCodec shortValCodec = new ShortValueCodec();
    while ((unpackedQ = shortValCodec.unPackQuality(dis)) != null) {
      unpackedQList.add(unpackedQ);
    }
    return unpackedQList;
  }

  public List<Long> deCompactTs(ByteArrayInputStream compressedData) throws IOException {
    List<Long> unpackedTsList = new ArrayList<Long>();
    DataInputStream dis = new DataInputStream(compressedData);
    Long unpackedTs;
    LongValueCodec longValCodec = new LongValueCodec();
    while ((unpackedTs = longValCodec.unPackTs(dis)) != null) {
      unpackedTsList.add(unpackedTs);
    }
    return unpackedTsList;
  }

  private boolean varBinaryToArray(ImmutableBytesWritable ptr, byte[] binaryValue, byte[] binaryQ,
      byte[] binaryTS, Long startTs, Long stopTs, int numSamples, Long uri, SortOrder sortOrder) {
    log.debug(String.format("TS in method[%d, %d]", startTs, stopTs));
    try {
      if (!dbService.hasConnection()) {
        dbService.openConnection();
        throw new SQLException("dbService.openConnection() Exception");
      }
      String dataType = dbService.getDataType(uri);
      valueCodec = TagDataUtils.getCodec(dataType);

      ByteArrayInputStream baValue = new ByteArrayInputStream(binaryValue);
      List<TagData> rawValues = valueCodec.decompress(baValue);

      ByteArrayInputStream baQ = new ByteArrayInputStream(binaryQ);
      List<Short> rawQ = deCompactQ(baQ);

      ByteArrayInputStream baTS = new ByteArrayInputStream(binaryTS);
      List<Long> rawTS = deCompactTs(baTS);

      ArrayList<String> list = new ArrayList<>();
      List<TagData> tduList = new ArrayList<TagData>();
      int rowsUncompacted = 0;
      for (int i = 0; i < numSamples; ++i) {
        String values = null;
        if (dataType.equals("String")) {
          values = rawValues.get(i).getVs();
        } else if (dataType.equals("Float")) {
          values = String.valueOf(rawValues.get(i).getVd());
        } else {
          values = String.valueOf(rawValues.get(i).getVl());
        }
        log.debug(String.format("Values[%d, %s, %d]", rawTS.get(i), values, rawQ.get(i)));

        log.debug(String.format("TS[%d, %d, %d]", rawTS.get(i), startTs, stopTs));
        log.debug(String.format("TS-String[%s, %s, %s]", new Timestamp(rawTS.get(i)).toString(),
          new Timestamp(startTs).toString(), new Timestamp(stopTs)).toString());

        log.debug(String.format("TS-UTC[%d, %d, %d]", Utils.convertToUTC(rawTS.get(i)),
          Utils.convertToUTC(startTs), Utils.convertToUTC(stopTs)));
        log.debug(String.format("TS-UTC-String[%s, %s, %s]",
          new Timestamp(Utils.convertToUTC(rawTS.get(i))).toString(),
          new Timestamp(Utils.convertToUTC(startTs)).toString(),
          new Timestamp(Utils.convertToUTC(stopTs)).toString()));
        Long rawTsInUtc = Utils.convertToUTC(rawTS.get(i));
        if (rawTsInUtc >= startTs && rawTsInUtc <= stopTs) {
          log.debug("In here");
          TagData tdu = new TagData();
          tdu.setUri(uri);
          tdu.setTs(new Timestamp(rawTS.get(i)));
          if (dataType.equals("String")) {
            tdu.setVs(rawValues.get(i).getVs());
          } else if (dataType.equals("Float")) {
            tdu.setVd(rawValues.get(i).getVd());
          } else {
            tdu.setVl(rawValues.get(i).getVl());
          }
          tdu.setQ(rawQ.get(i));
          tduList.add(tdu);
          log.debug("Raw Values are " + tdu.toString());
          // We have to convert the TS to UTC before returning it
          list.add(String.format("%s, %s, %d", (new Timestamp(rawTS.get(i))).toString(), values,
            rawQ.get(i)));
          log.debug("list element for " + tdu.getUri() + ": " + list.get(list.size() - 1));
          if (tduList.size() % 20000 == 0) {
            log.debug("Upsert " + tduList.size() + " records to " + UNCOMPACTED_TABLE);
            dbService.upsertUncompactedData(UNCOMPACTED_TABLE, tduList);
            tduList.clear();
          }
          rowsUncompacted++;
        }
      }
      if (!tduList.isEmpty()) {
        log.debug("Upsert " + tduList.size() + " records to " + UNCOMPACTED_TABLE);
        dbService.upsertUncompactedData(UNCOMPACTED_TABLE, tduList);
      }
      dbService.closeConnection();
      log.debug("list size: " + list.size());
      String[] array = new String[list.size()];
      array = list.toArray(array);

      // PhoenixArray phoenixArray = new PhoenixArray(PVarchar.INSTANCE, array);
      // ptr.set(PVarcharArray.INSTANCE.toBytes(phoenixArray, PVarchar.INSTANCE, sortOrder));
      byte[] byteValue = getDataType().toBytes("Uncompacted " + rowsUncompacted + " records for "
          + uri + " between " + (new Timestamp(startTs)).toString() + " and "
          + (new Timestamp(stopTs)).toString() + " in " + UNCOMPACTED_TABLE);
      ptr.set(byteValue);
      return true;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public PDataType getDataType() {
    // return PVarcharArray.INSTANCE;
    return PVarchar.INSTANCE;
  }

  /**
   * Determines whether or not a function may be used to form the start/stop key of a scan
   * @return the zero-based position of the argument to traverse into to look for a primary key
   *         column reference, or {@value #NO_TRAVERSAL} if the function cannot be used to form the
   *         scan key.
   */
  public int getKeyFormationTraversalIndex() {
    return NO_TRAVERSAL;
  }

  /**
   * Manufactures a KeyPart used to construct the KeyRange given a constant and a comparison
   * operator.
   * @param childPart the KeyPart formulated for the child expression at the
   *          {@link #getKeyFormationTraversalIndex()} position.
   * @return the KeyPart for constructing the KeyRange for this function.
   */
  public KeyPart newKeyPart(KeyPart childPart) {
    return null;
  }

  /**
   * Determines whether or not the result of the function invocation will be ordered in the same way
   * as the input to the function. Returning true enables an optimization to occur when a GROUP BY
   * contains function invocations using the leading PK column(s).
   * @return true if the function invocation will preserve order for the inputs versus the outputs
   *         and false otherwise
   */
  public OrderPreserving preservesOrder() {
    return OrderPreserving.NO;
  }

  @Override
  public SortOrder getSortOrder() {
    return children.get(0).getSortOrder();
  }

}
