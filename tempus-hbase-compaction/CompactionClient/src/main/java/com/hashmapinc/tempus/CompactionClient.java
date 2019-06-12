package com.hashmapinc.tempus;

import com.google.common.collect.Lists;

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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


public class CompactionClient implements AsyncInterface {

    private static final Logger log = Logger.getLogger(CompactionClient.class);

    private static final String PROPERTY_FILE_SHORT_OPTION = "p";
    private static final String PROPERTY_FILE_LONG_OPTION = "props";
    private static final String PID_FILE_SHORT_OPTION = "r";
    private static final String PID_FILE_LONG_OPTION = "runfile";
    private static Options CMD_LINE_OPTS = new Options();

    static {
        CMD_LINE_OPTS.addOption(PROPERTY_FILE_SHORT_OPTION, PROPERTY_FILE_LONG_OPTION, true,
                "URL of the properties file. e.g. /etc/conf/properties/compaction.properties");
        CMD_LINE_OPTS.addOption(PID_FILE_SHORT_OPTION, PID_FILE_LONG_OPTION, true,
                "PID file to write process id of current process. e.g. /var/run/compactionclient");
    }

    /**
     * url for the hbase zookeeper connection
     */
    private static final String HBASE_ZOOKEEPER_URL_PROPERTY = "hbase.zookeeper.url";
    /**
     * Phoenix tables for querying data
     */
    private static final String PHOENIX_TAG_DATA_LIST_TABLE_NAME_PROPERTY = "compaction.tdlist.table";
    private static final String PHOENIX_TAG_DATA_TABLE_NAME_PROPERTY = "compaction.tagdata.table";
    private static final String PHOENIX_TAG_DATA_COMPACT_TABLE_NAME_PROPERTY =
            "compaction.tagdata.compact.table";
    private static final String COMPACTION_TIME_WINDOW_SECS_PROPERTY =
            "compaction.data.window.seconds";
    private static final String COMPACTION_ENDTS_PROPERTY = "compaction.num.uncompacted.days";

    /**
     * default amount of time between connection retries
     */
    private static final long DEFAULT_RETRY_MILLIS = 2000L;
    private static final int DEFAULT_NUM_THREADS = 16;
    private static final int DEFAULT_COMPACTION_PARTITIONS = 100;
    private static final int DEFAULT_DELETE_PARTITIONS = 50;
    private static final long DEFAULT_PHOENIX_QUERY_TIMEOUT_MS = 60000;

    private long retryMillis = DEFAULT_RETRY_MILLIS;

    private static String hbaseZookeeperUrl;
    private static String tableName;

    private static Table table = null;
    private static Connection conn = null;
    private static long phoenixQueryTimeOutMs = DEFAULT_PHOENIX_QUERY_TIMEOUT_MS;

    private static Integer compactionWindowTimeInSecs;
    private static long upsertBatchSize;

    private static long numUnCompactedMilliSecs;

    private static AtomicLong totalTdRecordsToBeProcessed = new AtomicLong(0L);
    private static long totalTdRecordsCompacted = 0L;
    private static long totalTdRecordsUpserted = 0L;
    private static long totalTdRecordsDeleted = 0L;

    private static int executorShutdownAwaitTime = 10;
    /**
     * @return the numUnCompactedMilliSecs
     */
    public static long getCompactionEndTsMillis() {
        return numUnCompactedMilliSecs;
    }

    /**
     * @return the hbaseZookeeperUrl
     */
    public String getHbaseZookeeperUrl() {
        return hbaseZookeeperUrl;
    }

    /**
     * @param hbaseZookeeperUrl the hbaseZookeeperUrl to set
     */
    public static void setHbaseZookeeperUrl(String hbaseZookeeperUrl) throws ConfigurationException {
        if ((hbaseZookeeperUrl == null) || (hbaseZookeeperUrl.length() == 0)) {
            throw new IllegalArgumentException("setHbaseZookeeperUrl");
        }
        Utils.validateHbaseZookeeperUrl(hbaseZookeeperUrl);
        CompactionClient.hbaseZookeeperUrl = hbaseZookeeperUrl;
    }

    private static void initHBaseConnection() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(conf);
        table = conn.getTable(TableName.valueOf(tableName.toUpperCase()));
        phoenixQueryTimeOutMs = conf.getLong("phoenix.query.timeoutMs", DEFAULT_PHOENIX_QUERY_TIMEOUT_MS);
    }

    private static void releaseHBaseConn() throws IOException {
        table.close();
        conn.close();
    }

    /**
     * @return the compactionWindowTimeInMins
     */
    public static Integer getCompactionWindowTimeInSecs() {
        return compactionWindowTimeInSecs;
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

    private static CommandLine readCommandLineArgs(String[] args, Options options) {
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
                Utils.getValueFromArgs(commandLine, CompactionClient.PROPERTY_FILE_LONG_OPTION,
                        CompactionClient.PROPERTY_FILE_SHORT_OPTION);
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

    private static DatabaseService initDbConn() {
        DatabaseService dbService = null;
        try {
            dbService = new DatabaseService(CompactionClient.hbaseZookeeperUrl);
            DatabaseService.openConnection();
            log.info("DB Connection success");
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("IllegalArgumentException: " + e.getMessage());
        } catch (SQLException e) {
            throw new RuntimeException("SQLException: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClassNotFoundException: " + e.getMessage());
        }
        return dbService;
    }

    private static void loadCompactionProperties(final CommandLine commandLine, final Properties properties) {
        try {
            if (commandLine == null) {
                throw new IllegalArgumentException("commandLine");
            }
            if (properties == null) {
                throw new IllegalArgumentException("properties");
            }

            long ONE_SEC_IN_MILLIS = 1000L;
            long ONE_DAY_IN_SECS = (24 * 60 * 60);
            CompactionClient.setHbaseZookeeperUrl(Utils.readProperty(properties, HBASE_ZOOKEEPER_URL_PROPERTY));
            CompactionClient.tableName = Utils.readProperty(properties, PHOENIX_TAG_DATA_TABLE_NAME_PROPERTY);
            CompactionClient.compactionWindowTimeInSecs = Integer.parseInt(Utils.readProperty(properties, COMPACTION_TIME_WINDOW_SECS_PROPERTY));
            //CompactionClient.upsertBatchSize = Long.parseLong(Utils.readProperty(properties,COMPACTION_BATCH_UPSERTS_SIZE_PROPERTY, String.valueOf(DEFAULT_BATCH_UPSERTS)));
            int numUnCompactedDays =
                    Integer.parseInt(Utils.readProperty(properties, COMPACTION_ENDTS_PROPERTY, "0"));
            CompactionClient.numUnCompactedMilliSecs = numUnCompactedDays * (ONE_DAY_IN_SECS) * (ONE_SEC_IN_MILLIS);
            if (log.isDebugEnabled()) {
                log.info("hbaseZookeeperUrl: " + CompactionClient.hbaseZookeeperUrl);
                log.info("tableName: " + CompactionClient.tableName);
                log.info("compactionWindowTimeInSecs: " + CompactionClient.compactionWindowTimeInSecs);
                //log.info("upsertBatchSize: " + CompactionClient.upsertBatchSize);
                log.info("numUnCompactedMilliSecs: " + CompactionClient.numUnCompactedMilliSecs);
            }
        } catch (ConfigurationException e) {
            throw new RuntimeException("ConfigurationException: " + e.getMessage());
        }
    }

    private static void loadDbProperties(final CommandLine commandLine, final Properties properties) {
        if (commandLine == null) {
            throw new IllegalArgumentException("commandLine");
        }
        if (properties == null) {
            throw new IllegalArgumentException("properties");
        }
        DatabaseService.setTagListTable(Utils.readProperty(properties, PHOENIX_TAG_DATA_LIST_TABLE_NAME_PROPERTY));
        DatabaseService.setTagDataTable(Utils.readProperty(properties, PHOENIX_TAG_DATA_TABLE_NAME_PROPERTY));
        DatabaseService.setCompactionTable(Utils.readProperty(properties, PHOENIX_TAG_DATA_COMPACT_TABLE_NAME_PROPERTY));
        //DatabaseService.setCompactionStatusTable(Utils.readProperty(properties,PHOENIX_COMPACTION_STATUS_TABLE_NAME_PROPERTY));
        if (log.isDebugEnabled()) {
            log.info("tagListTable: " + DatabaseService.getTagListTable());
            log.info("tagDataTable: " + DatabaseService.getTagDataTable());
            log.info("compactionTable: " + DatabaseService.getCompactionTable());
          //  log.info("compactionStatusTable: " + DatabaseService.getCompactionStatusTable());
        }
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

    static CompletableFuture<List<List<Map<Long, Long[]>>>> compactData(final Properties
                                                                                properties, final
                                                                        ExecutorService executor, final DatabaseService dbService, final long startTs, final long endTs) {
        final int partitionSizeForCompaction = Integer.valueOf(Utils.readProperty(properties,
                "compaction.partition", String.valueOf(DEFAULT_COMPACTION_PARTITIONS)));
        CompletableFuture<List<List<Map<Long, Long[]>>>> compactedUriStats = AsyncInterface.
                getCompactionTagList(executor, dbService).thenCompose(tagList -> {
            CompactionClient.totalTdRecordsToBeProcessed.set(tagList.size());
            log.info("Num of uris to be compacted:- " + CompactionClient.totalTdRecordsToBeProcessed);
            List<List<TagList>> compactionList = Lists.partition(tagList, partitionSizeForCompaction);
            return AsyncInterface
                    .compactAllUriPartitions(executor, compactionList, dbService, startTs,
                            endTs, CompactionClient.getCompactionWindowTimeInSecs(), CompactionClient.table)
                    .thenCompose((List<CompletableFuture<List<Map<Long, Long[]>>>> allDeleteListFutures) -> {
                        return CompletableFuture.allOf(allDeleteListFutures.toArray(new
                                CompletableFuture[allDeleteListFutures.size()]))
                                .thenApply(aVoid -> {
                                    return allDeleteListFutures.stream().map(fut -> fut.join()).collect(Collectors.toList());
                                });
                    });
        });
        return compactedUriStats;
    }

    static List<Long> collectUrisToBeDeleted(List<List<Map<Long, Long[]>>> compactedData) {
        List<Long> urisToBeDeleted = new ArrayList<>();
        long numUrisToBeDeleted = 0;
        for (List<Map<Long, Long[]>> allDeletes : compactedData) {
            for (Map<Long, Long[]> compactedStatsMap : allDeletes) {
                long deleteUri = compactedStatsMap.keySet().stream().findFirst().get();
                long uriCompactedRecords = compactedStatsMap.get(deleteUri)[0];
                long uriUpsertedRecords = compactedStatsMap.get(deleteUri)[1];
                if (uriUpsertedRecords > 0) {
                    urisToBeDeleted.add(deleteUri);
                    ++numUrisToBeDeleted;
                    log.info("URI [" + deleteUri + "]; Compacted: [" + uriCompactedRecords + "]; Upserted: ["
                            + uriUpsertedRecords + "];");
                }
                CompactionClient.totalTdRecordsCompacted += uriCompactedRecords;
                CompactionClient.totalTdRecordsUpserted += uriUpsertedRecords;
            }
        }
        return urisToBeDeleted;
    }

    static long deleteCompactedUris(final Properties properties, final ExecutorService executor,
                                    final DatabaseService dbService, final long startTs, final long endTs, final List<Long> urisToBeDeleted,
                                    final long numUrisToBeDeleted) throws ExecutionException,
            InterruptedException {
        int partitionSizeForDeletes = Integer.valueOf(Utils.readProperty(properties, "compaction.deletes.batchsize", String.valueOf(DEFAULT_DELETE_PARTITIONS)));
        long totalTdRecordsDeleted = 0;
        int retry_count = 0;
        int reduce_factor = 10;
        int partitionSize = (int) Math.min(partitionSizeForDeletes, numUrisToBeDeleted);
        while (partitionSize != 0) {
            totalTdRecordsDeleted = deleteUris(executor, dbService, startTs, endTs,
                    urisToBeDeleted, partitionSize);
            if (totalTdRecordsDeleted > 0)
                break;
            partitionSize = Math.round(partitionSize / 2);
            log.info("Recalculated partitionSize: " + partitionSize);
        }
        log.info("totalTdRecordsDeleted: " + totalTdRecordsDeleted);
        return totalTdRecordsDeleted;
    }

    private static long deleteUris(Executor executor, final DatabaseService dbService, final
    long startTs, final long endTs, final List<Long> urisToBeDeleted, int partitionSizeForDeletes)
            throws ExecutionException, InterruptedException {
        List<List<Long>> deleteList = Lists.partition(urisToBeDeleted, partitionSizeForDeletes);
        //Block here for timeout of phoenixQueryTimeOutMs * the number of batch deletes
        final long waitTimeMinsDeletion = (CompactionClient.phoenixQueryTimeOutMs + 1) * deleteList.size();
        if(log.isDebugEnabled()) {
            log.debug("deleteList size: " + deleteList.size());
            log.debug("Deletion Wait Time: " + waitTimeMinsDeletion + " ms");
        }
        CompletableFuture<List<Long>> futureRecordsDeletedForCompactedPts = AsyncInterface.deleteCompactedRecords(executor, deleteList, dbService, startTs, endTs);
        List<Long> listDeletedRecords = null;
        try {
            listDeletedRecords = futureRecordsDeletedForCompactedPts.get(waitTimeMinsDeletion, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.info("Got TimeoutException exception: " + e.getMessage());
            return -1;
        }
        return listDeletedRecords.stream().mapToLong(deletedRecords -> deletedRecords).sum();
    }

    public static void main(String[] args) throws ConfigurationException {
        long compactionClientStartTime = System.currentTimeMillis();
        long startTimeForCompaction = 0L;
        long stopTimeForCompaction = 0L;
        long startTimeForDeletes = 0L;
        long stopTimeForDeletes = 0L;

        final CommandLine commandLine = readCommandLineArgs(args, CompactionClient.CMD_LINE_OPTS);
        if (commandLine == null)
            throw new RuntimeException("Exception while parsing command line args.");

        Boolean deletePidFile = deletePidFile(commandLine);
        if (deletePidFile == null)
            throw new RuntimeException("One instance of compaction is running. Please stop it before running a new one");

        final Properties properties = loadProperties(commandLine, CompactionClient.CMD_LINE_OPTS);
        if (properties == null)
            throw new RuntimeException("Exception while loading properties.");

        loadCompactionProperties(commandLine, properties);

        loadDbProperties(commandLine, properties);


        final DatabaseService dbService = initDbConn();
        if (dbService == null)
            throw new RuntimeException("Exception in intializing DB service: dbService is null");

        final int numThreads = Integer.valueOf(Utils.readProperty(properties, "compaction.numthreads", String.valueOf(DEFAULT_NUM_THREADS)));
        log.info("Num of Threads: " + numThreads);

        final ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            CompactionClient.initHBaseConnection();
            if (log.isTraceEnabled()) {
                log.trace("cpEpcClient.getCompactionEndTs(): " + CompactionClient.getCompactionEndTsMillis());
                log.trace("Current TS: " + (new Timestamp(compactionClientStartTime)).toString());
                log.trace("Old TS: " + (new Timestamp(compactionClientStartTime - CompactionClient.getCompactionEndTsMillis())).toString());
            }

            //Start Time will be always be passed as 0.
            long startTs = 0;
            // If there is an endTs configured in properties file we will only compact till currenttime-(configured TS)
            long compactionEndTs = CompactionClient.getCompactionEndTsMillis();
            long endTs = (compactionEndTs > 0) ? (compactionClientStartTime - compactionEndTs) : compactionClientStartTime;

            startTimeForCompaction = System.currentTimeMillis();
            CompletableFuture<List<List<Map<Long, Long[]>>>> compactedDataFuture = compactData
                    (properties, executor, dbService, startTs, endTs);
            //Collect All Values
            List<List<Map<Long, Long[]>>> compactedData = compactedDataFuture.get();
            stopTimeForCompaction = System.currentTimeMillis();


            List<Long> urisToBeDeleted = collectUrisToBeDeleted(compactedData);

            long numUrisToBeDeleted = urisToBeDeleted.size();
            log.info("urisToBeDeleted size: " + numUrisToBeDeleted);

            startTimeForDeletes = System.currentTimeMillis();
            if (numUrisToBeDeleted > 0) {
                totalTdRecordsDeleted = deleteCompactedUris(properties, executor, dbService, startTs, endTs, urisToBeDeleted, numUrisToBeDeleted);
            }
            stopTimeForDeletes = System.currentTimeMillis();

            CompactionClient.releaseHBaseConn();

            log.info("Calling executor shutdown....");
            executor.shutdown();
            log.info("Waiting termination of executor....");
            executor.awaitTermination(executorShutdownAwaitTime, TimeUnit.MINUTES);
        } catch (IOException e) {
            // HBase Exception
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (!executor.isTerminated()) {
                log.debug("Executor not terminated yet. Calling shutdownNow()...");
            }
            executor.shutdownNow();
            DatabaseService.closeConnection();
        }

        if (deletePidFile) {
            String pidFile = Utils.getValueFromArgs(commandLine, PID_FILE_LONG_OPTION, PID_FILE_SHORT_OPTION);
            Utils.deletePidFile(pidFile);
        }


        log.info("=====Compaction Stats=====");
        log.info("Total Tags fetched for Compaction: " + CompactionClient.totalTdRecordsToBeProcessed.get());
        log.info("Total Records Compacted: " + CompactionClient.totalTdRecordsCompacted);
        log.info("Total Records Upserted: " + CompactionClient.totalTdRecordsUpserted);
        log.info("Total Records Deleted: " + CompactionClient.totalTdRecordsDeleted);
        log.info("Time Taken to fetch Point Tags, Compact & Upsert Compacted Data : " +
                (stopTimeForCompaction - startTimeForCompaction) + " ms");
        log.info("Time Taken for deleting compacted Records: " + (stopTimeForDeletes - startTimeForDeletes) + " ms");
        log.info("Total Time Taken: " + (System.currentTimeMillis() - compactionClientStartTime) + " ms");
    }
}