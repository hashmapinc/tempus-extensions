package com.hashmapinc.tempus;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;
import com.hashmapinc.tempus.CompactionProtos.CompactedData;
import com.hashmapinc.tempus.CompactionProtos.CompactionRequest;
import com.hashmapinc.tempus.CompactionProtos.CompactionResponse;
import com.hashmapinc.tempus.CompactionProtos.CompactionService;

import javax.xml.crypto.Data;

public class CompactionClient implements AsyncInterface{

  final static Logger log = Logger.getLogger(CompactionClient.class);

  private static final String PROPERTY_FILE_SHORT_OPTION = "p";
  private static final String PROPERTY_FILE_LONG_OPTION = "props";
  private static final String PID_FILE_SHORT_OPTION = "r";
  private static final String PID_FILE_LONG_OPTION = "runfile";

  /** url for the hbase zookeeper connection */
  public static final String HBASE_ZOOKEEPER_URL_PROPERTY = "hbase.zookeeper.url";
  /** Phoenix tables for querying data */
  public static final String PHOENIX_TAG_DATA_LIST_TABLE_NAME_PROPERTY = "compaction.tdlist.table";
  public static final String PHOENIX_TAG_DATA_TABLE_NAME_PROPERTY = "compaction.tagdata.table";
  public static final String PHOENIX_TAG_DATA_COMPACT_TABLE_NAME_PROPERTY =
      "compaction.tagdata.compact.table";
  public static final String PHOENIX_COMPACTION_STATUS_TABLE_NAME_PROPERTY =
      "compaction.tagdata.compactstatus.table";
  public static final String COMPACTION_TIME_WINDOW_SECS_PROPERTY =
      "compaction.data.window.seconds";
  public static final String COMPACTION_BATCH_UPSERTS_SIZE_PROPERTY = "compaction.batch.upserts";
  public static final String COMPACTION_BATCH_UPSERTS_TYPE_PROPERTY = "compaction.upserts.jdbc";
  public static final String COMPACTION_ENDTS_PROPERTY = "compaction.num.uncompacted.days";

  public static int NUM_RETRIES_CONNECTING_TO_DATABASE = 7;
  public static long ONE_SEC_IN_MILLIS = 1000L;
  private static long ONE_DAY_IN_SECS = (24 * 60 * 60);
  // public static final long DEFAULT_
  public static final long DEFAULT_BATCH_UPSERTS = 15000;

  /** default amount of time between connection retries */
  private static final long DEFAULT_RETRY_MILLIS = 2000L;
  // private static boolean timeToExit = false;

  private long retryMillis = DEFAULT_RETRY_MILLIS;
  private Properties properties;
  private CommandLine commandLine;
  private String hbaseZookeeperUrl;
  private String tableName;

  private DatabaseService dbService = null;
  //TODO
  private static Table table = null;
  private Configuration conf = null;
  private Connection conn = null;
  private CompactionRequest request = null;
  private Integer compactionWindowTimeInSecs;
  private long upsertBatchSize;
  private List<TagDataCompressed> tdcList = new ArrayList<TagDataCompressed>();
  private Boolean jdbcUpserts = false;
  private long compEndTs;
  //TODO
  private static String DEFAULT_NUM_THREADS = "20";


  /**
   * @return the properties
   */
  public Properties getProperties() {
    return properties;
  }

  /**
   * @param properties the properties to set
   */
  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  /**
   * @return the commandLine
   */
  public CommandLine getCommandLine() {
    return commandLine;
  }

  /**
   * @param commandLine the commandLine to set
   */
  public void setCommandLine(CommandLine commandLine) {
    this.commandLine = commandLine;
  }

  /**
   * @return the jdbcUpserts
   */
  public Boolean getJdbcUpserts() {
    return jdbcUpserts;
  }

  /**
   * @param jdbcUpserts the jdbcUpserts to set
   */
  public void setJdbcUpserts(Boolean jdbcUpserts) {
    this.jdbcUpserts = jdbcUpserts;
  }

  /**
   * @return the compEndTs
   */
  public long getCompEndTs() {
    return compEndTs;
  }

  /**
   * @param compEndTs the compEndTs to set
   */
  public void setCompEndTs(long compEndTs) {
    this.compEndTs = compEndTs;
  }

  public void upsertCompactedData() throws Exception {
    if (tdcList.size() > 0) {
      log.debug("Persisting " + tdcList.size() + " compacted Tag Data Records.");
      dbService.upsertCompactedRecords(tdcList, jdbcUpserts);
      tdcList.clear();
    }
  }

  /**
   * @return the upsertBatchSize
   */
  public long getUpsertBatchSize() {
    return upsertBatchSize;
  }

  /**
   * @param upsertBatchSize the upsertBatchSize to set
   */
  public void setUpsertBatchSize(long upsertBatchSize) {
    this.upsertBatchSize = upsertBatchSize;
  }

  /**
   * @return the retryMillis
   */
  public long getRetryMillis() {
    return retryMillis;
  }

  /**
   * @param retryMillis the retryMillis to set
   */
  public void setRetryMillis(long retryMillis) {
    this.retryMillis = retryMillis;
  }

  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return the dbService
   */
  public DatabaseService getDbService() {
    return dbService;
  }

  /**
   * @param dbService the dbService to set
   */
  public void setDbService(DatabaseService dbService) {
    this.dbService = dbService;
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
    if ((hbaseZookeeperUrl == null) || (hbaseZookeeperUrl.length() == 0)) {
      throw new IllegalArgumentException("setHbaseZookeeperUrl");
    }
    Utils.validateHbaseZookeeperUrl(hbaseZookeeperUrl);
    this.hbaseZookeeperUrl = hbaseZookeeperUrl;
  }

  private void initHBaseConnection() throws IOException {

    conf = HBaseConfiguration.create();
    // conf.setInt("hbase.client.retries.number", 3);
    // conf.setInt("zookeeper.session.timeout", 60000);
    // conf.setInt("zookeeper.recovery.retry", 0);
    // log.debug("Quorum is:- " + conf.get("hbase.zookeeper.quorum"));
    conn = ConnectionFactory.createConnection(conf);
    table = conn.getTable(TableName.valueOf(tableName.toUpperCase()));
  }

  private void releaseHBaseConn() throws IOException {
    table.close();
    conn.close();
  }


  /**
   * @return the compactionWindowTimeInMins
   */
  public Integer getCompactionWindowTimeInSecs() {
    return compactionWindowTimeInSecs;
  }

  /**
   * @param compactionWindowTimeInMins the compactionWindowTimeInMins to set
   */
  public void setCompactionWindowTimeInSecs(String compactionWindowTimeInMins) {
    this.compactionWindowTimeInSecs = Integer.parseInt(compactionWindowTimeInMins);
  }

  public long getCompactionWindowTimeInMillis() {
    return (this.compactionWindowTimeInSecs * ONE_SEC_IN_MILLIS);
  }

  private static final void usage(Options CMD_LINE_OPTS) {

    HelpFormatter h = new HelpFormatter();
    PrintWriter p = new PrintWriter(System.err);
    p.println();
    p.println();
    h.printUsage(p, HelpFormatter.DEFAULT_WIDTH, CompactionClient.class.getName(), CMD_LINE_OPTS);
    h.printOptions(p, HelpFormatter.DEFAULT_WIDTH, CMD_LINE_OPTS,
      HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD);
    p.flush();
  }

  public CompactionClient(CommandLine commandLine, Properties properties)
      throws ConfigurationException {
    if (commandLine == null) {
      throw new IllegalArgumentException("commandLine");
    }
    if (properties == null) {
      throw new IllegalArgumentException("properties");
    }
    this.commandLine = commandLine;
    this.properties = properties;

    tableName = Utils.readProperty(properties, PHOENIX_TAG_DATA_TABLE_NAME_PROPERTY);
    setHbaseZookeeperUrl(Utils.readProperty(properties, HBASE_ZOOKEEPER_URL_PROPERTY));
    compactionWindowTimeInSecs =
        Integer.parseInt(Utils.readProperty(properties, COMPACTION_TIME_WINDOW_SECS_PROPERTY));
    upsertBatchSize = Long.parseLong(Utils.readProperty(properties,
      COMPACTION_BATCH_UPSERTS_SIZE_PROPERTY, String.valueOf(DEFAULT_BATCH_UPSERTS)));
    // By default we set jdbcUpsertsProp to false if property is absent
    jdbcUpserts = Boolean.parseBoolean(Utils.readProperty(properties,
      COMPACTION_BATCH_UPSERTS_TYPE_PROPERTY, String.valueOf(false)));
    int compactTillDays =
        Integer.parseInt(Utils.readProperty(properties, COMPACTION_ENDTS_PROPERTY, "0"));
    compEndTs = compactTillDays * (ONE_DAY_IN_SECS) * (ONE_SEC_IN_MILLIS);
  }

  private static Options getOptions(){
    Options CMD_LINE_OPTS = new Options();

    CMD_LINE_OPTS.addOption(PROPERTY_FILE_SHORT_OPTION, PROPERTY_FILE_LONG_OPTION, true,
            "URL of the properties file. e.g. /etc/conf/properties/compaction.properties");
    CMD_LINE_OPTS.addOption(PID_FILE_SHORT_OPTION, PID_FILE_LONG_OPTION, true,
            "PID file to write process id of current process. e.g. /var/run/compactionclient");
    return CMD_LINE_OPTS;
  }

  private static CommandLine readCommandLineArgs(String[] args, Options options){
    CommandLine commandLine = null;
    try {
      commandLine = new BasicParser().parse(options, args);
    } catch (ParseException e) {
      e.printStackTrace();
      usage(options);
      System.exit(-1);
    }
    return commandLine;
  }

  private static Properties loadProperties(CommandLine commandLine, Options options) {
    Properties properties = null;
    String propertiesURL =
            Utils.getValueFromArgs(commandLine, PROPERTY_FILE_LONG_OPTION, PROPERTY_FILE_SHORT_OPTION);
    if (propertiesURL == null) {
      usage(options);
      System.exit(-1);
    } else {
      properties = Utils.loadProperties(propertiesURL);
      if (properties == null) {
        log.error("Error loading properties at: " + propertiesURL);
        System.exit(-1);
      }
    }
    return properties;
  }

  private static DatabaseService initDbConn(final CommandLine commandLine, final Properties properties) {
    DatabaseService dbService = null;
    try {
      dbService = new DatabaseService(Utils.readProperty(properties, HBASE_ZOOKEEPER_URL_PROPERTY));
      dbService.openConnection();
      dbService.setTagListTable(Utils.readProperty(properties, PHOENIX_TAG_DATA_LIST_TABLE_NAME_PROPERTY));
      dbService.setTagDataTable(Utils.readProperty(properties, PHOENIX_TAG_DATA_TABLE_NAME_PROPERTY));
      dbService.setCompactionTable(Utils.readProperty(properties, PHOENIX_TAG_DATA_COMPACT_TABLE_NAME_PROPERTY));
      dbService.setCompactionStatusTable(Utils.readProperty(properties, PHOENIX_COMPACTION_STATUS_TABLE_NAME_PROPERTY));
      log.info("DB Connection success");
    } catch (ConfigurationException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return dbService;
  }

  private static CompactionClient initCompactionClient(final CommandLine commandLine, final Properties properties) {
    CompactionClient cpEpcClient = null;
    try {
      cpEpcClient = new CompactionClient(commandLine, properties);
      cpEpcClient.initHBaseConnection();
      log.info("HBase Connection success");
    } catch (ConfigurationException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return cpEpcClient;
  }

  private static Boolean deletePidFile(CommandLine commandLine) {
    String pidFile = Utils.getValueFromArgs(commandLine, PID_FILE_LONG_OPTION, PID_FILE_SHORT_OPTION);
    Boolean performPidCheck = (pidFile == null) ? false : true;
    if (performPidCheck) {
      // check if one instance is already compacting do not start compaction.
      if (Utils.isPreviousCompactionRunning(pidFile)) {
        log.error("One instance of compaction is running. Please stop it before running a new one");
        return null;
      } else {
        Utils.writePidFile(System.getProperty("pid"), pidFile);
        return true;
      }
    }
    return false;
  }

  public static void main(String[] args) {
    /*
     * Phoenix stores DATE/TIMESTAMP as UTC. Underlying HBase long values for DATE/TIMESTAMP are in
     * localtime So irrespective of datatypes we will pass the long values to coprocessor.
     * Timestamp.getTime() will return us the corresponding long values for TIMESTAMP values in
     * Phoenix Date.get..... will return us the corresponding long values for DATE values in Phoenix
     * As per Phoenix [https://phoenix.apache.org/language/datatypes.html#date_type]: Please note
     * that this DATE type is different than the DATE type as defined by the SQL 92 standard in that
     * it includes a time component. As such, it is not in compliance with the JDBC APIs
     */
    // This is always in local time
    long compactionClientStartTime = System.currentTimeMillis();
    Long totalTdRecordsCompacted = 0L;
    Long totalTdRecordsUpserted = 0L;
    long fetchTsCompactAndUpsertStartTime;
    long fetchTsCompactAndUpsertStopTime;
    long startTimeForFetchTsCompactAndUpsert;
    long stopTimeForFetchTsCompactAndUpsert;
    long startTimeForBatchDeletes;
    long stopTimeForBatchDeletes;

    Options options = getOptions();

    final CommandLine commandLine = readCommandLineArgs(args, options);
    if(commandLine == null)
      throw new RuntimeException("Exception while parsing command line args.");

    Boolean deletePidFile = deletePidFile(commandLine);
    if(deletePidFile == null)
      throw new RuntimeException("One instance of compaction is running. Please stop it before running a new one");

    final Properties properties =  loadProperties(commandLine, options);
    if(properties == null)
      throw new RuntimeException("Exception while loading properties.");


    final DatabaseService dbService = initDbConn(commandLine, properties);
    if(dbService == null)
      throw new RuntimeException("Exception in intializing DB service");

    final CompactionClient cpEpcClient = initCompactionClient(commandLine, properties);
    if(cpEpcClient == null)
      throw new RuntimeException("Exception in intializing Compaction client");
    cpEpcClient.setDbService(dbService);

    int numThreads = Integer.valueOf(Utils.readProperty(properties, "numthreads",
            DEFAULT_NUM_THREADS));
    //numThreads = Utils.readProperty(properties, System.getProperty("num.threads"),DEFAULT_NUM_THREADS);
    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {

      // Timestamp lastCompactionTime = dbService.queryLastCompactionTime();
      long startTs = 0;
      // (lastCompactionTime == null) ? 0 : lastCompactionTime.getTime();
      // log.info("cpEpcClient.getCompEndTs(): " + cpEpcClient.getCompEndTs());
      // log.info("Current TS: " + (new Timestamp(compactionClientStartTime)).toString());
      // log.info("Old TS: " + (new Timestamp(compactionClientStartTime -
      // cpEpcClient.getCompEndTs())).toString());
      // If there is an endTs configured in properties file we will only compact till currenttime -
      // (configured TS)
      long endTs = (cpEpcClient.getCompEndTs() > 0)
          ? (compactionClientStartTime - cpEpcClient.getCompEndTs()) : compactionClientStartTime;
      // System.exit(0);
      // Get distinct tags between start_time(lastCompactionTine OR 0) and end_time (currentTime)
      List<TagList> tagList =
          dbService.getDistinctURI(NUM_RETRIES_CONNECTING_TO_DATABASE, DEFAULT_RETRY_MILLIS);
      log.info("Num of uris to be compacted:- " + tagList.size());

      List<Long> pointTagsToBeDeleted = new ArrayList<>();
      List<List<TagList>> compactionList = Lists.partition(tagList, 20);


      CompletableFuture<List<List<Map<Long, Long[]>>>> allValuesFutures = AsyncInterface
              .compactAllUriPartitions(executor, compactionList,dbService,startTs,
                      endTs, cpEpcClient.getCompactionWindowTimeInSecs(), CompactionClient.table)
              .thenCompose((List<CompletableFuture<List<Map<Long, Long[]>>>> allDeleteListFutures) -> {
                return CompletableFuture.allOf(allDeleteListFutures.toArray(new
                        CompletableFuture[allDeleteListFutures.size()]))
                        .thenApply(aVoid -> {
                          return allDeleteListFutures.stream().map(fut -> fut.join()).collect(Collectors.toList());
                        });
              });

      //Blocking above call
      List<List<Map<Long, Long[]>>> allValues =  allValuesFutures.get();


      List<Long> urisToBeDeleted = new ArrayList<>();
      long numUrisToBeDeleted = 0;

      for( List<Map<Long, Long[]>> allDeletes : allValues){
        for (Map<Long, Long[]> compactedStatsMap : allDeletes) {
          long deleteUri = compactedStatsMap.keySet().stream().findFirst().get();
          long compactedRecords = compactedStatsMap.get(deleteUri)[0];
          long upsertedRecords = compactedStatsMap.get(deleteUri)[1];
          if (upsertedRecords > 0) {
            urisToBeDeleted.add(deleteUri);
            ++numUrisToBeDeleted;
            log.info("URI [" + deleteUri + "]; Compacted: [" + compactedRecords + "]; Upserted: ["
                    + upsertedRecords + "];");
          }
          totalTdRecordsCompacted += compactedRecords;
          totalTdRecordsUpserted += upsertedRecords;
        }
      }

      log.info("urisToBeDeleted size: " + numUrisToBeDeleted);
      //final String ptsAsString = pointTagsToBeDeleted.parallelStream().;
      List<List<Long>> deleteList = Lists.partition(urisToBeDeleted, 50);
      log.info("deleteList size: " + deleteList.size());
      //pointTagsToBeDeleted.clear();
      log.info("deleteList size: " + deleteList.size());
      //log.info("pointTagsToBeDeleted: " + ptsAsString);

      //TODO
      long phoenixQueryTimeOutMs = 60000;
      final long waitTimeMinsDeletion = ((phoenixQueryTimeOutMs * deleteList.size()) / 1000) / 60;
      log.info("Deletion Wait Time: " + waitTimeMinsDeletion + " mins");

      long asyncPipelineDeleteCompactedPointTagsStartTime = System.currentTimeMillis();
      CompletableFuture<List<Long>> futureRecordsDeletedForCompactedPts = AsyncInterface.deleteCompactedRecords(executor, deleteList, dbService, startTs, endTs);
      //Block here for timeout of phoenixQueryTimeOutMs * the number of batch deletes
      List<Long> listDeletedRecords = null;
      try {
        listDeletedRecords = futureRecordsDeletedForCompactedPts.get(waitTimeMinsDeletion, TimeUnit.MINUTES);
      } catch (TimeoutException e) {
        //TODO
        // if we can findout for which list timeout has occured. try to delete each uri
        // Again do a try/catch weith timeout anf if still a timeout occurs, write the delete
        // queries in a log file so that they can be manually deleted. How to notify about such
        // failures ?
        e.printStackTrace();
        listDeletedRecords = new ArrayList<>();
      }
      long asyncPipelineDeleteCompactedPointTagsStopTime = System.currentTimeMillis();

      long totalRecordsDeleted = 0l;
      for (long deletedRecords : listDeletedRecords) {
        totalRecordsDeleted += deletedRecords;
      }

      log.info("Total Records Compacted: " + totalTdRecordsCompacted);
      log.info("Total Records Deleted: " + totalRecordsDeleted);
      log.info("Time Taken for deleting compacted Records: " + (asyncPipelineDeleteCompactedPointTagsStopTime - asyncPipelineDeleteCompactedPointTagsStartTime) + " ms");

      if(true == false) {
        List<Long> compactedURIs = new ArrayList<Long>();
        for (TagList tl : tagList) {
          // Call coprocessor for each uri
          long tdRecordsCompacted =
                  cpEpcClient.compactWindowedData(tl.getId(), startTs, endTs, tl.getDataType());
          log.info("[" + tdRecordsCompacted + "] records compacted for [" + tl.getId() + "];");
          totalTdRecordsCompacted += tdRecordsCompacted;
          if (tdRecordsCompacted > 0) compactedURIs.add(tl.getId());
        }
        cpEpcClient.upsertCompactedData();
        log.info("Total TD records compacted:- " + totalTdRecordsCompacted);
      }

      if (totalTdRecordsCompacted > 0) {
        CompactionStatus compactionStatus =
                new CompactionStatus(totalTdRecordsCompacted);
        compactionStatus.setLastCompactionTs(new Timestamp(endTs));
        dbService.upsertCompactionStatus(compactionStatus);
      }

      cpEpcClient.releaseHBaseConn();

      log.info("Calling executor shutdown....");
      executor.shutdown();
      // Wait 10 mins max for anytasks to complete
      executor.awaitTermination(10, TimeUnit.MINUTES);
    } catch (SQLException e) {
      // Phoenix SQL Exception
      e.printStackTrace();
    } catch (IOException e) {
      // HBase Exception
      e.printStackTrace();
    } catch (ConfigurationException e) {
      // Configuration exception in case of HBase ZK Url
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }finally {
      if (!executor.isTerminated()) {
        log.info("Executor not terminated yet. Apply some diagnostics");
      }
      executor.shutdownNow();
      dbService.closeConnection();
    }

    if (deletePidFile) {
      String pidFile = Utils.getValueFromArgs(commandLine, PID_FILE_LONG_OPTION, PID_FILE_SHORT_OPTION);
      Utils.deletePidFile(pidFile);
    }
    log.info(
            String.format("Time taken by %s for compacting [%d] records [%d ms]", CompactionClient.class,
                    totalTdRecordsCompacted, (System.currentTimeMillis() - compactionClientStartTime)));
  }

  @Deprecated
  public long compactWindowedData(long uri, final long startTs, final long endTs,
                                  final String dataType) throws Exception {
    long tdRecordsCompacted = 0L;
    TagData td = dbService.getMinMaxTs(uri);

    if (td == null || td.getMinTs() == null || td.getMaxTs() == null) {
      log.info("No Compaction run for [" + uri + "] as TD details/min(TS)/max(TS) is null");
      return 0;
    }
    long minTs = td.getMinTs().getTime();
    long maxTs = td.getMaxTs().getTime();

    long windowStartTs = (startTs == 0) ? minTs : startTs;
    long windowEndTs = windowStartTs + getCompactionWindowTimeInMillis();

    String logStartTs = new Timestamp(windowStartTs).toString();
    String logEndTs = new Timestamp(windowEndTs).toString();
    log.info("Compaction run for [" + uri + "] from [" + logStartTs + " - " + logEndTs + "];");

    while ((endTs - windowStartTs) > 0) {
      String logWindowStartTs = new Timestamp(windowStartTs).toString();
      String logWindowEndTs = new Timestamp(windowEndTs).toString();
      log.debug("Compacting for time window[" + logWindowStartTs + " - " + logWindowEndTs + "];");

      Map<byte[], CompactionResponse> compactionResp =
              AsyncInterface.callCompactionRpc(uri, windowStartTs, windowEndTs, dataType);
      if (compactionResp != null) {
        log.debug("PB response is " + compactionResp.toString());
        log.debug("CompactionResponse Size = " + compactionResp.size());
        // log.info("CompactionResponse Key = " + compactionResp.keySet().toString());
        for (CompactionResponse response : compactionResp.values()) {
          if (response.hasPackedData()) {
            TagDataCompressed tdc = new TagDataCompressed();
            CompactedData compactData = response.getPackedData();
            if (uri == compactData.getUri()) {
              tdc.setId(compactData.getUri());
              tdc.setTs(compactData.getTs().toByteArray());
              tdc.setQ(compactData.getQuality().toByteArray());
              tdc.setVb(compactData.getVb().toByteArray());
              tdc.setNs(compactData.getNumSamples());
              tdc.setStTs(new Timestamp(compactData.getFirstptTs()));
              // Upsert TS in Xtd_compact table for this compacted PT record
              tdc.setUpTs(new Timestamp(System.currentTimeMillis()));
              log.debug("TDC = " + tdc.toString());
              tdcList.add(tdc);
              if (tdcList.size() % upsertBatchSize == 0) {
                upsertCompactedData();
              }
              tdRecordsCompacted += compactData.getNumSamples();
            } else {
              log.error("Compaction request was made for uri: " + uri
                      + " but recvd response for uri: " + compactData.getUri());
              throw new IllegalStateException("Compaction request was made for uri: " + uri
                      + " but recvd response for uri: " + compactData.getUri());
            }
          } else {
            // Coprocessor recvd some exception while processing data.
            // isFail will be true and exception will be in response.getErrMsg()
            if (response.getIsFail()) {
              log.error("Received exception with error messsage " + response.getErrMsg());
              throw new Exception(response.getErrMsg());
            } else {
              // In case of no results present for time window we set isFail to false
              // Also error_msg is set with NO_ERR:....
              if (response.hasErrMsg() && response.getErrMsg().startsWith("NO_ERR")) {
                log.info("Error msg:- " + response.getErrMsg());
              } else {
                // Should never come here as we have handled failures in exception
                // If @all we reach here throw an exception
                // This block happens when ERR(no_packed_data) && ERR(no_fail) && ERR(no_err_msg)
                // For now, lets log(ERROR) and continue to the next compaction window for the TD
                log.error(
                        "Unhandled state  ERR(no_packed_data) && ERR(no_fail) && ERR(no_err_msg).");
                // throw new IllegalStateException("E(no_packed_data) && E(no_fail) &&
                // E(no_err_msg)");
              }
            }
          }
        }
      } else {
        log.error("Check RS logs. Compaction Service returned null responses for TimeWindow.:"
                + windowStartTs + " - " + windowEndTs);
      }
      if (windowEndTs > maxTs) {
        break;
      }

      windowStartTs = windowEndTs;
      windowEndTs = windowStartTs + getCompactionWindowTimeInMillis();
    }
    log.info("Done Compaction run for [" + uri + "]from[" + logStartTs + " - " + logEndTs + "];");

    return tdRecordsCompacted;
  }
}