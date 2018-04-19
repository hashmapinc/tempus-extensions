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
 * select UNCOMPACT("VB", "Q", "TS", "NS", 'T1', 'T2', "ID")from td_compact [where id = X and
 * STTS <=T2 and STTS >=T1]
 */
/*
 * We need to pass the foll parameters UNCOMPACT(VARBINARY, VARBINARY, VARBINARY, INTEGER, BIGINT,
 * BIGINT) Usage:- UNCOMPACT("VB", "Q", "TS", "NUMSAMPLES", FROMTIME, ENDTIME, "ID")
 * @param VARBINARY :- VB - Compacted Values
 * @param VARBINARY :- Q - Compacted Quality
 * @param VARBINARY :- TS - Compacted Timestamps
 * @param INTEGER   :- NS - Number of Compacted Samples
 * @param VARCHAR   :- Start TS - from time where we need the records
 * @param VARCHAR   :- End TS - time till which we need records
 * @param BIGINT    :- URI - used to upsert the values to tduc (tag_data_uncompact) table
 */
@FunctionParseNode.BuiltInFunction(name = Uncompact.NAME,
    args = { @FunctionParseNode.Argument(allowedTypes = { PVarbinary.class }),
        @FunctionParseNode.Argument(allowedTypes = { PVarbinary.class }),
        @FunctionParseNode.Argument(allowedTypes = { PVarbinary.class }),
        @FunctionParseNode.Argument(allowedTypes = { PInteger.class }),
        @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }),
        @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }),
        @FunctionParseNode.Argument(allowedTypes = { PInteger.class }) })
public class Uncompact extends ScalarFunction {
  public static final String NAME = "UNCOMPACT";
  private static final String UNCOMPACTED_TABLE = "TDUC";
  private static final Logger log = Logger.getLogger(Uncompact.class);

  private int estimatedByteSize = 0;
  private static ValueCodec valueCodec = null;
  private static DatabaseService dbService = null;
  private static String unCompactTable;


  public Uncompact() {
  }

  public static void initDbConn() {
    try {
      String hbaseZookeeperUrl = System.getenv("PHOENIX_CONN_PARAM");
      if ((null == hbaseZookeeperUrl) || (0 == hbaseZookeeperUrl.length())) {
        throw new ConfigurationException("Please set PHOENIX_CONN_PARAM environment variable with value as Zookeeper Quorum");
      }
      dbService = new DatabaseService(hbaseZookeeperUrl);
      log.info("hbaseZookeeperUrl: " + hbaseZookeeperUrl);
      DatabaseService.openConnection();
      log.info("DB Connection success");
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("IllegalArgumentException: " + e.getMessage());
    } catch (SQLException e) {
      throw new RuntimeException("SQLException: " + e.getMessage());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("ClassNotFoundException: " + e.getMessage());
    } catch (ConfigurationException e){
      throw new RuntimeException("ConfigurationException: " + e.getMessage());
    }
  }

  public static List<Short> deCompactQ(ByteArrayInputStream compressedData) throws IOException {
    List<Short> unpackedQList = new ArrayList<Short>();
    DataInputStream dis = new DataInputStream(compressedData);
    Short unpackedQ;
    ShortValueCodec shortValCodec = new ShortValueCodec();
    while ((unpackedQ = shortValCodec.unPackQuality(dis)) != null) {
      unpackedQList.add(unpackedQ);
    }
    return unpackedQList;
  }

  public static List<Long> deCompactTs(ByteArrayInputStream compressedData) throws IOException {
    List<Long> unpackedTsList = new ArrayList<Long>();
    DataInputStream dis = new DataInputStream(compressedData);
    Long unpackedTs;
    LongValueCodec longValCodec = new LongValueCodec();
    while ((unpackedTs = longValCodec.unPackTs(dis)) != null) {
      unpackedTsList.add(unpackedTs);
    }
    return unpackedTsList;
  }

  public Uncompact(List<Expression> children) throws SQLException, ConfigurationException {
    super(children);
    initDbConn();
    if (dbService == null)
      throw new RuntimeException("Exception in intializing DB service");

    String tagListTable = System.getenv("TAGLIST_TABLE");
    if ((null == tagListTable) || (0 == tagListTable.length())) {
      throw new RuntimeException("Please set TAGLIST_TABLE environment variable with value of " +
              "table to find URI datatype.");
    }
    DatabaseService.setTagListTable(tagListTable);

    unCompactTable = System.getenv("UNCOMPACT_TABLE");
    if ((null == unCompactTable) || (0 == unCompactTable.length())) {
      unCompactTable = UNCOMPACTED_TABLE;
    }
    // Drop Table if exists and Create again so that on every UDF execution we have data in it
    dbService.dropTable(unCompactTable);
    dbService.createTable(unCompactTable);
  }

  private Expression getExpression(int index) {
    return children.get(index);
  }

  private byte[] getBinaryData(Expression binaryExpr, Tuple tuple, ImmutableBytesWritable ptr) {
    estimatedByteSize = PVarbinary.INSTANCE.estimateByteSize(
      (byte[]) binaryExpr.getDataType().toObject(ptr, binaryExpr.getSortOrder()));
    byte[] binaryValue = (byte[]) binaryExpr.getDataType().toObject(ptr, binaryExpr.getSortOrder(),
      estimatedByteSize, 0);
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
      if(log.isDebugEnabled())
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

    Expression uriExpr = getExpression(6);
    if (!uriExpr.evaluate(tuple, ptr)) {
      return false;
    } else if (ptr.getLength() == 0) {
      return true;
    }
    Long uri = (Long) uriExpr.getDataType().toObject(ptr, uriExpr.getSortOrder());

    if(log.isDebugEnabled()){
      log.info(String.format("%s, %s", startTs.toString(), stopTs.toString()));
      log.info(String.format("TS in long[%d, %d]", startTs.getTime(), stopTs.getTime()));
      log.debug("Tuple size: " + tuple.size());
    }
    return varBinaryToArray(ptr, binaryValue, binaryQ, binaryTS, startTs.getTime(),
      stopTs.getTime(), numSamples, uri, getSortOrder());
  }

  private boolean varBinaryToArray(ImmutableBytesWritable ptr, byte[] binaryValue, byte[] binaryQ,
      byte[] binaryTS, Long startTs, Long stopTs, int numSamples, Long uri, SortOrder sortOrder) {
    if(log.isDebugEnabled())
      log.info(String.format("TS in method[%d, %d]", startTs, stopTs));
    if (!dbService.hasConnection()) {
      throw new RuntimeException("dbService.hasConnection() Exception");
    }
    try {
      String dataType = dbService.getDataType(uri);
      log.info("dataType1: " + dataType);
      valueCodec = TagDataUtils.getCodec(dataType);

      ByteArrayInputStream baValue = new ByteArrayInputStream(binaryValue);
      List<TagData> rawValues = valueCodec.decompress(baValue);
      log.info("rawValues: " + rawValues);

      ByteArrayInputStream baQ = new ByteArrayInputStream(binaryQ);
      List<Short> rawQ = deCompactQ(baQ);

      ByteArrayInputStream baTS = new ByteArrayInputStream(binaryTS);
      List<Long> rawTS = deCompactTs(baTS);

      List<TagData> tducList = new ArrayList<TagData>();
      long rowsUncompacted = 0;
      for (int i = 0; i < numSamples; ++i) {
        String values = null;
        if (dataType.equals("String")) {
          values = rawValues.get(i).getVs();
        } else if (dataType.equals("Float")) {
          values = String.valueOf(rawValues.get(i).getVd());
        } else {
          values = String.valueOf(rawValues.get(i).getVl());
        }

        if(log.isDebugEnabled()){
          log.info(String.format("Values[%d, %s, %d]", rawTS.get(i), values, rawQ.get(i)));

          log.info(String.format("TS[%d, %d, %d]", rawTS.get(i), startTs, stopTs));
          log.info(String.format("TS-String[%s, %s, %s]", new Timestamp(rawTS.get(i)).toString(),
                  new Timestamp(startTs).toString(), new Timestamp(stopTs)).toString());

          log.info(String.format("TS-UTC[%d, %d, %d]", Utils.convertToUTC(rawTS.get(i)),
                  Utils.convertToUTC(startTs), Utils.convertToUTC(stopTs)));
          log.info(String.format("TS-UTC-String[%s, %s, %s]",
                  new Timestamp(Utils.convertToUTC(rawTS.get(i))).toString(),
                  new Timestamp(Utils.convertToUTC(startTs)).toString(),
                  new Timestamp(Utils.convertToUTC(stopTs)).toString()));
        }
        Long rawTsInUtc = Utils.convertToUTC(rawTS.get(i));

        if (rawTsInUtc >= startTs && rawTsInUtc <= stopTs) {
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
          tducList.add(tdu);
          rowsUncompacted++;

          if(log.isDebugEnabled())
            log.info("Raw Values are " + tdu.toString());

          if (rowsUncompacted % 20000 == 0) {
            int rowsUpserted = dbService.upsertUncompactedData(unCompactTable, tducList);
            if(log.isDebugEnabled())
              log.info("Upserted " + rowsUpserted + " records to " + unCompactTable);
            tducList.clear();
          }
        }
      }
      if (!tducList.isEmpty()) {
        int rowsUpserted = dbService.upsertUncompactedData(unCompactTable, tducList);
        if(log.isDebugEnabled())
          log.info("Upserted " + rowsUpserted + " records to " + unCompactTable);
      }
      DatabaseService.closeConnection();

      byte[] byteValue = getDataType().toBytes("Uncompacted " + rowsUncompacted + " records for "
          + uri + " between " + (new Timestamp(startTs)).toString() + "and "
          + (new Timestamp(stopTs)).toString() + " in " + unCompactTable);
      ptr.set(byteValue);
      return true;
    } catch (IOException e) {
      if(log.isDebugEnabled())
        log.info(NAME + " UDF IOException: " + e.getMessage());
    } catch (SQLException e) {
      if(log.isDebugEnabled())
        log.info(NAME + " UDF SQLException: " + e.getMessage());
    } catch (Exception e) {
      if(log.isDebugEnabled())
        log.info(NAME + " UDF Exception: " + e.getMessage());
    }
    return false;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public PDataType getDataType() {
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
