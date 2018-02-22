package com.hashmapinc.tempus;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

//import com.hashmapinc.tempus.FacilityCompactionStatus;
import com.hashmapinc.tempus.TagData;
import com.hashmapinc.tempus.TagDataCompressed;
import com.hashmapinc.tempus.Utils;

public class DatabaseService {

  private transient static Logger log = Logger.getLogger(DatabaseService.class);

  private static Integer EPOCH_START_TIME = 18000;

  private transient Connection dbConnection;
  private transient PreparedStatement queryAssetDataStatusStmt;
  private transient PreparedStatement queryDistinctTagDataStmt;
  private transient PreparedStatement queryDistinctTagListStmt;
  private transient PreparedStatement queryMinMaxTagDataStmt;
  private transient PreparedStatement queryTagDataDataTypeStmt;

  private transient PreparedStatement upsertTagDataCompressedStmt;
  private transient PreparedStatement deleteTagDataStmt;
  private transient PreparedStatement upsertCompactionStatusStmt;
  private transient PreparedStatement upsertTduStmt;
  
  private String tagListTable;
  private String tagDataTable;
  private String compactionTable;
  private String compactionStatusTable;
  
  private SQLContext sqlContext;
  private transient JavaSparkContext sparkContext;
  /////////////////////////////////////////
  private boolean tagDataJdbc = true;
  private String phoenixJdbcUrl;
  private String hbaseZookeeperUrl;


  /**
   * @return the tagListTable
   */
  public String getTagListTable() {
    return tagListTable;
  }

  /**
   * @param tagDataListTable the tagDataListTable to set
   */
  public void setTagListTable(String tagListTable) {
    this.tagListTable = tagListTable;
  }

  /**
   * @return the tagDataTable
   */
  public String getTagDataTable() {
    return tagDataTable;
  }

  /**
   * @param tagDataTable the tagDataTable to set
   */
  public void setTagDataTable(String tagDataTable) {
    if (tagDataTable == null || tagDataTable.length() == 0) {
      throw new IllegalArgumentException("tagDataTable");
    }
    this.tagDataTable = tagDataTable.toLowerCase();
  }

  /**
   * @return the compactionTable
   */
  public String getCompactionTable() {
    return compactionTable;
  }

  /**
   * @param compactionTable the compactionTable to set
   */
  public void setCompactionTable(String compactionTable) {
    if (compactionTable == null || compactionTable.length() == 0) {
      throw new IllegalArgumentException("compactionTable");
    }
    this.compactionTable = compactionTable.toLowerCase();
  }
  
  /**
   * @return the compactionStatusTable
   */
  public String getCompactionStatusTable() {
    return compactionStatusTable;
  }

  /**
   * @param compactionStatusTable the compactionStatusTable to set
   */
  public void setCompactionStatusTable(String compactionStatusTable) {
    if (compactionStatusTable == null || compactionStatusTable.length() == 0) {
      throw new IllegalArgumentException("compactionStatusTable");
    }
    this.compactionStatusTable = compactionStatusTable.toLowerCase();
  }

  /**
   * @return the phoenixJdbcUrl
   */
  public String getPhoenixJdbcUrl() {
    return phoenixJdbcUrl;
  }

  /**
   * @param phoenixJdbcUrl the phoenixJdbcUrl to set
   */
  public void setPhoenixJdbcUrl(String jdbcUrl) {
    if (jdbcUrl == null || jdbcUrl.length() == 0) {
      throw new IllegalArgumentException("jdbcUrl");
    }
    this.phoenixJdbcUrl =
        jdbcUrl.startsWith("jdbc:phoenix:") ? jdbcUrl : ("jdbc:phoenix:" + jdbcUrl);
  }

  /**
   * @return the hbaseZookeeperUrl
   */
  public String getHbaseZookeeperUrl() {
    return hbaseZookeeperUrl;
  }

  /**
   * @param hbaseZookeeperUrl the hbaseZookeeperUrl to set
   * @throws ConfigurationException
   */
  public void setHbaseZookeeperUrl(String hbaseZookeeperUrl) throws ConfigurationException {
    if (hbaseZookeeperUrl == null) {
      throw new IllegalArgumentException("hbaseZookeeperUrl");
    }
    Utils.validateHbaseZookeeperUrl(hbaseZookeeperUrl);
    this.hbaseZookeeperUrl = hbaseZookeeperUrl;
  }

  /**
   * @return the dbConnection
   */
  protected Connection getDbConnection() {
    return dbConnection;
  }

  /**
   * @param dbConnection the dbConnection to set
   */
  protected void setDbConnection(Connection dbConnection) {
    if (hbaseZookeeperUrl == null) {
      throw new IllegalArgumentException("dbConnection");
    }
    this.dbConnection = dbConnection;
  }

  public void openConnection() throws Exception {
    String jdbcUrl = getPhoenixJdbcUrl();
    if (jdbcUrl == null) {
      throw new IllegalStateException("can't open connection, no jdbc url defined");
    }

    if (dbConnection != null) {
      closeConnection();
    }

    // TODO
    // we shouldnt need this yet to check
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
    dbConnection = DriverManager.getConnection(jdbcUrl);
    dbConnection.setAutoCommit(true);

    if (log.isDebugEnabled()) {
      log.debug("Opened connection to: " + jdbcUrl);
    }
  }

  public void closeConnection() {
    if (dbConnection != null) {
      try {
        dbConnection.close();

        if (log.isDebugEnabled()) {
          log.debug("Closed Connection to: " + getPhoenixJdbcUrl());
        }

      } catch (Exception ex) {
        log.error("Error closing connection: ", ex);
      } finally {
        clearPreparedStatements();
      }
    }
  }

  /**
   * Test if the connection is opened
   * @return
   * @throws SQLException
   */
  public boolean hasConnection() {
    if (dbConnection != null) {
      return true;
    }
    return false;
  }

  /**
   * @return the sqlContext
   */
  private SQLContext getSqlContext() {
    return sqlContext;
  }

  /**
   * @param sqlContext the sqlContext to set
   */
  private void setSqlContext(SQLContext sqlContext) {
    this.sqlContext = sqlContext;
  }

  /**
   * @return the sparkContext
   */
  private JavaSparkContext getSparkContext() {
    return sparkContext;
  }

  /**
   * @param sparkContext the sparkContext to set / private void setSparkContext(JavaSparkContext
   *          sparkContext) { this.sparkContext = sparkContext; } /**
   * @return the tagDataJdbc
   */
  public boolean isTagDataJdbc() {
    return tagDataJdbc;
  }

  /**
   * @param tagDataJdbc the tagDataJdbc to set
   */
  public void setTagDataJdbc(boolean tagDataJdbc) {
    this.tagDataJdbc = tagDataJdbc;
  }

  public DatabaseService() throws ConfigurationException {
    String hbaseZookeeperUrl = System.getenv("PHOENIX_CONN_PARAM");
    if ((null == hbaseZookeeperUrl) || (0 == hbaseZookeeperUrl.length())) {
      throw new ConfigurationException(
          "Please set PHOENIX_CONN_PARAM environment variable with value as Zookeeper Quorum");
    }
    this.hbaseZookeeperUrl = hbaseZookeeperUrl;
    setPhoenixJdbcUrl(hbaseZookeeperUrl);
  }
  
  /**
   * @param hbaseZookeeperUrl
   * @throws ConfigurationException
   */
  public DatabaseService(String hbaseZookeeperUrl) throws ConfigurationException {
    this.hbaseZookeeperUrl = hbaseZookeeperUrl;
    setPhoenixJdbcUrl(hbaseZookeeperUrl);
    SparkConf spkConf = new SparkConf().setAppName(DatabaseService.class.getName());
    String masterUrl = spkConf.get("spark.master", "local[8]");
    spkConf.setMaster(masterUrl);
    sparkContext = new JavaSparkContext(spkConf);
    setSqlContext(new SQLContext(sparkContext));
  }

  private Map<String, String> createSparkOptions(String table) {
    if (table == null) {
      throw new IllegalArgumentException("table");
    }
    if (getHbaseZookeeperUrl() == null) {
      throw new IllegalStateException("hbaseZookeeperUrl has not been set");
    }

    Map<String, String> sparkOptions = new HashMap<String, String>();
    sparkOptions.put("zkUrl", getHbaseZookeeperUrl());
    sparkOptions.put("table", table);
    return sparkOptions;
  }

  protected void clearPreparedStatements() {
    queryAssetDataStatusStmt = null;
    queryDistinctTagListStmt = null;
    queryDistinctTagDataStmt = null;
    queryMinMaxTagDataStmt = null;
    upsertTagDataCompressedStmt = null;
    deleteTagDataStmt = null;
    upsertCompactionStatusStmt = null;
    upsertTduStmt = null;
  }

  public Timestamp queryLastCompactionTime() throws SQLException {
    if (!hasConnection()) {
      throw new IllegalStateException("no connection");
    }

    if (queryAssetDataStatusStmt == null) {
      queryAssetDataStatusStmt =
          getDbConnection().prepareStatement("SELECT lcts FROM " + compactionStatusTable);
    } else {
      queryAssetDataStatusStmt.clearParameters();
    }

    long start = System.currentTimeMillis();
    ResultSet results = queryAssetDataStatusStmt.executeQuery();
    if (!results.next()) {
      return null;
    }

    Timestamp lastCompactionTime = results.getTimestamp(1);

    if (log.isDebugEnabled()) {
      log.debug("Queried CompactionStatus Status: " + (System.currentTimeMillis() - start) + "ms");
    }
    return lastCompactionTime;
  }

  public List<TagList> getDistinctURI(int numRetries, long retryAfterMillis) {
    for (int i = 0; i < numRetries; i++) {
      try {
        List<TagList> tagList = queryTagList();
        return tagList;
      } catch (Exception e) {
        log.error("Error getting asset list: ", e);
      }

      try {
        log.info("Retrying in: " + (retryAfterMillis * (i + 1)) + "ms.");
        Thread.sleep(retryAfterMillis * (i + 1));
      } catch (InterruptedException e) {
        log.error("Interrupted retrying: ", e);
        return null;
      }
    }
    return null;
  }
  
  public List<TagList> queryTagList() throws SQLException {
    if (!hasConnection()) {
      throw new IllegalStateException("no connection");
    }

    if (queryDistinctTagListStmt == null) {
      queryDistinctTagListStmt = getDbConnection().prepareStatement("SELECT id, datatype from "
          + tagListTable + " where status != 0 ");
    } else {
      queryDistinctTagListStmt.clearParameters();
    }

    List<TagList> uris = new ArrayList<TagList>();
    long start = System.currentTimeMillis();
    ResultSet results = queryDistinctTagListStmt.executeQuery();

    while (results.next()) {
      TagList tl = new TagList();
      tl.setId(results.getInt(1));
      tl.setDataType(results.getString(2));
      uris.add(tl);
      if (log.isTraceEnabled()) {
        log.trace("TagList: " + tl.toString());
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("Queried distinct URI : " + uris.size() + " records in "
          + (System.currentTimeMillis() - start) + "ms.");
    }
    return uris;
  }
  
  public List<Integer> getDistinctURI(int numRetries, long retryAfterMillis, long startTs,
      long endTs) {
    for (int i = 0; i < numRetries; i++) {
      try {
        List<Integer> uris = queryTagData(startTs, endTs);
        return uris;
      } catch (Exception e) {
        log.error("Error getting asset list: ", e);
      }

      try {
        log.info("Retrying in: " + (retryAfterMillis * (i + 1)) + "ms.");
        Thread.sleep(retryAfterMillis * (i + 1));
      } catch (InterruptedException e) {
        log.error("Interrupted retrying: ", e);
        return null;
      }
    }
    return null;
  }

  public List<Integer> queryTagData(long startTs, long endTs) throws SQLException {
    if (!hasConnection()) {
      throw new IllegalStateException("no connection");
    }

    if (queryDistinctTagDataStmt == null) {
      queryDistinctTagDataStmt = getDbConnection().prepareStatement("SELECT DISTINCT(id) from "
          + tagDataTable + " where ts <= TO_TIMESTAMP(?) and ts >= TO_TIMESTAMP(?)");
    } else {
      queryDistinctTagDataStmt.clearParameters();
    }

    Timestamp startTime = new Timestamp(convertToUTC(startTs));
    Timestamp endTime = new Timestamp(convertToUTC(endTs));

    queryDistinctTagDataStmt.setString(1, endTime.toString());
    queryDistinctTagDataStmt.setString(2, startTime.toString());
    log.info("startTime:- " + startTime.toString() + "; endTime:- " + endTime.toString());
    List<Integer> uris = new ArrayList<Integer>();
    long start = System.currentTimeMillis();
    ResultSet results = queryDistinctTagDataStmt.executeQuery();

    while (results.next()) {
      uris.add(results.getInt(1));
      if (log.isTraceEnabled()) {
        log.trace("TagData: " + results.getString(1));
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("Queried distinct URI : " + uris.size() + " records in "
          + (System.currentTimeMillis() - start) + "ms.");
    }
    return uris;
  }

  public TagData getMinMaxTs(long uri) throws SQLException {
    if (!hasConnection()) {
      throw new IllegalStateException("no connection");
    }

    if (queryMinMaxTagDataStmt == null) {
      queryMinMaxTagDataStmt = getDbConnection()
          .prepareStatement("SELECT MIN(ts), MAX(ts) FROM " + tagDataTable + " where id = ?");
    } else {
      queryMinMaxTagDataStmt.clearParameters();
    }

    long start = System.currentTimeMillis();
    queryMinMaxTagDataStmt.setLong(1, uri);
    ResultSet results = queryMinMaxTagDataStmt.executeQuery();
    if (!results.next()) {
      return null;
    }

    TagData uriDetails = new TagData();
    uriDetails.setMinTs(results.getTimestamp(1));
    uriDetails.setMaxTs(results.getTimestamp(2));

    if (log.isDebugEnabled()) {
      log.debug(
        "Queried min and max TS for uri: " + (System.currentTimeMillis() - start) + "ms. : ");
    }
    return uriDetails;
  }

  public void upsertCompactedRecords(List<TagDataCompressed> tdcList, Boolean jdbcUpserts)
      throws Exception {
    if (tdcList == null) {
      throw new IllegalArgumentException("tdcList");
    }

    if (jdbcUpserts) {
      upsertCompactedPointTagsJdbc(tdcList);
    } else {
      writeTagDataCompressed(sparkContext.parallelize(tdcList));
    }
  }

  public void upsertCompactedPointTagsJdbc(List<TagDataCompressed> compressedPointTags)
      throws SQLException {

    if (!hasConnection()) {
      throw new IllegalStateException("no connection");
    }

    if (upsertTagDataCompressedStmt != null) upsertTagDataCompressedStmt.clearParameters();

    dbConnection.setAutoCommit(false);
    int numRowsUpserted = 0;
    long start = System.currentTimeMillis();
    for (TagDataCompressed tdc : compressedPointTags) {
      if (tdc == null) {
        throw new IllegalArgumentException("tagDataCompressed");
      }
      upsertTagDataCompressedStmt = getDbConnection().prepareStatement("UPSERT INTO "
          + compactionTable + " (id, stts, vb, q, ts, ns, upts) " + " VALUES(?, ?, ?, ?, ?, ?, ?)");
      upsertTagDataCompressedStmt.setLong(1, tdc.getId());
      upsertTagDataCompressedStmt.setDate(2, new Date(tdc.getStTs().getTime()));
      upsertTagDataCompressedStmt.setBytes(3, tdc.getVb());
      upsertTagDataCompressedStmt.setBytes(4, tdc.getQ());
      upsertTagDataCompressedStmt.setBytes(5, tdc.getTs());
      upsertTagDataCompressedStmt.setLong(6, tdc.getNs());
      upsertTagDataCompressedStmt.setDate(7, new Date(System.currentTimeMillis()));

      numRowsUpserted += upsertTagDataCompressedStmt.executeUpdate();
    }
    dbConnection.commit();
    if (log.isDebugEnabled()) {
      log.info("Upserted [" + numRowsUpserted + "] Tag Data Compacted records. Completed  in "
          + (System.currentTimeMillis() - start) + "ms.");
    }
    dbConnection.setAutoCommit(true);

  }

  private void writeTagDataCompressed(JavaRDD<TagDataCompressed> tagData) throws Exception {

    if (tagData == null) {
      throw new IllegalArgumentException("tagData");
    }

    tagData.cache();
    DataFrame dataFrame = getSqlContext().createDataFrame(tagData, TagDataCompressed.class);
    long start = System.currentTimeMillis();
    dataFrame.write().format("org.apache.phoenix.spark").mode(SaveMode.Overwrite)
        .options(createSparkOptions(compactionTable)).save();
    //log.debug("DF Count " + dataFrame.count());
    if (log.isDebugEnabled()) {
      log.debug(
        "TagDataCompressed DataFrame Write in: " + (System.currentTimeMillis() - start) + "ms. ");
    }
    tagData.unpersist();
  }

  public void writeTagDataCompressed(List<JavaRDD<TagDataCompressed>> tagDataList)
      throws Exception {

    if (tagDataList == null) {
      throw new IllegalArgumentException("tagDataList");
    }

    log.debug("Calling DF upserts for list size  " + tagDataList.size());
    JavaRDD<TagDataCompressed> compactedRDDs = null;
    for (JavaRDD<TagDataCompressed> tdc : tagDataList) {
      if (compactedRDDs == null) {
        compactedRDDs = tdc;
      } else {
        compactedRDDs = compactedRDDs.union(tdc);
      }
    }
    writeTagDataCompressed(compactedRDDs);
  }

  public void upsertCompactedPointTags(List<TagDataCompressed> compressedPointTags)
      throws Exception {

    log.debug("Calling DF upserts for list size  " + compressedPointTags.size());
    JavaRDD<TagDataCompressed> compactedRDD = sparkContext.parallelize(compressedPointTags);
    writeTagDataCompressed(compactedRDD);
  }

  public void upsertCompactionStatus(CompactionStatus compactionStatus)
      throws SQLException {
    if (!hasConnection()) {
      throw new IllegalStateException("no connection");
    }

    if (upsertCompactionStatusStmt == null) {
      upsertCompactionStatusStmt = getDbConnection().prepareStatement(
        "UPSERT into " + compactionStatusTable + " (lcts, numcomp) values (?, ?)");
    } else {
      upsertCompactionStatusStmt.clearParameters();
    }
    long start = System.currentTimeMillis();
    upsertCompactionStatusStmt.setDate(1,
      new Date(compactionStatus.getLastCompactionTs().getTime()));
    upsertCompactionStatusStmt.setLong(2, compactionStatus.getNumCompactedRecords());

    upsertCompactionStatusStmt.executeUpdate();

    if (log.isDebugEnabled()) {
      log.debug("Upsert AssetDataStatus time: " + (System.currentTimeMillis() - start) + "ms.");
    }
  }

  public void deleteCompactedURIs(List<Long> compactedURIs, long startTs, long endTs)
      throws SQLException {
    if (!hasConnection()) {
      throw new IllegalStateException("no connection");
    }

    if ((compactedURIs == null) || (compactedURIs.size() == 0))
      throw new IllegalArgumentException("tagDataCompressed");
    long start = System.currentTimeMillis();

    // if (deleteTagDataStmt != null) deleteTagDataStmt.clearParameters();
    int delURIsSize = compactedURIs.size();
    if (deleteTagDataStmt == null) {
      StringBuilder sql = new StringBuilder();
      sql.append("DELETE FROM " + tagDataTable
          + " WHERE ts >= TO_TIMESTAMP(?) AND ts <= TO_TIMESTAMP(?) AND id in (");
      for (int i = 0; i < delURIsSize; i++) {
        sql.append("?");
        if (i + 1 < delURIsSize) {
          sql.append(",");
        }
      }
      sql.append(")");
      deleteTagDataStmt = getDbConnection().prepareStatement(sql.toString());
    } else {
      deleteTagDataStmt.clearParameters();
    }

    // convert to utc because startTs and endTs are in current time zone
    Timestamp delStartTime = new Timestamp(startTs == 0 ? convertToUTC(EPOCH_START_TIME) : convertToUTC(startTs));
    Timestamp delEndTime = new Timestamp(convertToUTC(endTs));

    //log.info("delStartTime:" + delStartTime.toString());
    //log.info("delEndTime:" + delEndTime.toString());
    //log.info("delStartTime:" + delStartTime.getTime());
    //log.info("delEndTime:" + delEndTime.getTime());
    String stTime = delStartTime.toString();
    String endTime = delEndTime.toString();
    deleteTagDataStmt.setString(1, stTime);
    deleteTagDataStmt.setString(2, endTime);
    for (int i = 0; i < delURIsSize; i++) {
      deleteTagDataStmt.setLong(i + 1 + 2, compactedURIs.get(i));
    }

    //log.debug("Delete Statement: " + deleteTagDataStmt);
    int numRowsDeleted = deleteTagDataStmt.executeUpdate();
    if (log.isDebugEnabled()) {
      log.debug("Deleted [" + numRowsDeleted + "] Tag Data records for ["
          + compactedURIs.size() + "] tags in " + (System.currentTimeMillis() - start) + "ms.");
    }
  }

  private long convertToUTC(long ts) {
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(ts);
    // System.out.println("c.getTime() is : "+ c.getTime());
    // System.out.println("long ts is : "+ ts);

    TimeZone z = c.getTimeZone();
    int offset = z.getRawOffset();
    if (z.inDaylightTime(new Date(ts))) {
      offset = offset + z.getDSTSavings();
    }
    int offsetHrs = offset / 1000 / 60 / 60;
    int offsetMins = offset / 1000 / 60 % 60;

    // System.out.println("offset: " + offsetHrs);
    // System.out.println("offset: " + offsetMins);

    c.add(Calendar.HOUR_OF_DAY, (-offsetHrs));
    c.add(Calendar.MINUTE, (-offsetMins));

    // System.out.println("GMT Time: "+c.getTime() + " ; long-> " + c.getTimeInMillis());
    return c.getTimeInMillis();
  }

  public String getDataType(long uri) throws SQLException {
    // select from TAG_LIST where uri
    String uriDataType = null;
    if (!hasConnection()) {
      throw new IllegalStateException("no connection");
    }

    if (queryTagDataDataTypeStmt == null) {
      queryTagDataDataTypeStmt = getDbConnection()
          .prepareStatement("SELECT datatype FROM " + tagListTable + " where id = ?");
    } else {
      queryTagDataDataTypeStmt.clearParameters();
    }

    long start = System.currentTimeMillis();
    queryTagDataDataTypeStmt.setLong(1, uri);
    ResultSet results = queryTagDataDataTypeStmt.executeQuery();
    if (!results.next()) {
      return null;
    }
    uriDataType = results.getString(1);

    if (log.isDebugEnabled()) {
      log.debug("Queried datatype for uri: " + (System.currentTimeMillis() - start) + "ms. : ");
    }
    return uriDataType;
  }

  public void dropTable(String uncompactedTable) throws SQLException {
    Statement stmt = null;
    if (!hasConnection()) {
      throw new IllegalStateException("no connection");
    }
    if (uncompactedTable == null) {
      throw new IllegalArgumentException("Table name can't be null ");
    }
    stmt = dbConnection.createStatement();
    stmt.executeUpdate("DROP TABLE IF EXISTS " + uncompactedTable);
  }

  public void createTable(String uncompactedTable) throws SQLException {
    Statement stmt = null;
    if (!hasConnection()) {
      throw new IllegalStateException("no connection");
    }
    if (uncompactedTable == null) {
      throw new IllegalArgumentException("Table name can't be null ");
    }
    stmt = dbConnection.createStatement();
    stmt.executeUpdate("CREATE TABLE " + uncompactedTable
        + " (id BIGINT NOT NULL, ts DATE NOT NULL, vl BIGINT, vd DOUBLE, vs VARCHAR, q SMALLINT CONSTRAINT pk PRIMARY KEY (id, ts ROW_TIMESTAMP)) COMPRESSION = 'SNAPPY'");
  }

  public void upsertUncompactedData(String tableName, List<TagData> tduList) throws SQLException {
    if (!hasConnection()) {
      throw new IllegalStateException("no connection");
    }

    if (tableName == null) {
      throw new IllegalArgumentException("td");
    }

    if (upsertTduStmt != null) upsertTduStmt.clearParameters();

    int numRowsUpserted = 0;
    long start = System.currentTimeMillis();

    log.debug("list size is " + tduList.size());
    for (TagData td : tduList) {
      if (td == null) {
        throw new IllegalArgumentException("td");
      }
      upsertTduStmt = getDbConnection().prepareStatement(
        "UPSERT INTO " + tableName + " (id, ts, vl, vd, vs, q) " + " VALUES(?, ?, ?, ?, ?, ?)");
      upsertTduStmt.setLong(1, td.getUri());
      upsertTduStmt.setDate(2, new Date(td.getTs().getTime()));
      upsertTduStmt.setLong(3, td.getVl());
      upsertTduStmt.setDouble(4, td.getVd());
      upsertTduStmt.setString(5, td.getVs());
      upsertTduStmt.setShort(6, td.getQ());
      numRowsUpserted += upsertTduStmt.executeUpdate();
    }

    dbConnection.commit();
    if (log.isDebugEnabled()) {
      log.info("Upserted [" + numRowsUpserted + "] Tag Data Compacted records. Completed  in "
          + (System.currentTimeMillis() - start) + "ms.");
    }
    dbConnection.setAutoCommit(true);
  }
  
}
