package com.hashmapinc.tempus;

import com.hashmapinc.tempus.codec.ValueCodec;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

// If any new changes are made to the, use below for testing instead of re-deploying UDF.
// Deploying UDF for a single change is a cumbersome process for now. This saves us the hassle.
public class UncompactTest {
    public static final String NAME = "UNCOMPACT";
    private static final String UNCOMPACTED_TABLE = "TDUC";
    private static final Logger log = Logger.getLogger(UncompactTest.class);

    private int estimatedByteSize = 0;
    private static ValueCodec valueCodec = null;
    private static DatabaseService dbService = null;
    private static String unCompactTable;

    public static void main(String[] args) {
        if ((args == null) || (args.length < 2)) {
            log.info("Expect 2 arguments: 1.URI; 2. Compaction Table Name");
            throw new RuntimeException("Command Line arguements not specified.");
        }

        DatabaseService.setCompactionTable(args[0]);
        long uri = Long.valueOf(args[1]);

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
        } catch (ConfigurationException e) {
            throw new RuntimeException("ConfigurationException: " + e.getMessage());
        }

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
        try {
            dbService.dropTable(unCompactTable);
            dbService.createTable(unCompactTable);

            String dataType = dbService.getDataType(uri);
            valueCodec = TagDataUtils.getCodec(dataType);

            byte[] binaryValue = dbService.getCompactedData(uri, "vb");
            byte[] binaryQ = dbService.getCompactedData(uri, "q");
            byte[] binaryTS = dbService.getCompactedData(uri, "ts");

            if (binaryValue == null)
                throw new RuntimeException("binaryValue is null");

            ByteArrayInputStream baValue = new ByteArrayInputStream(binaryValue);
            List<TagData> rawValues = valueCodec.decompress(baValue);

            ByteArrayInputStream baQ = new ByteArrayInputStream(binaryQ);
            List<Short> rawQ = Uncompact.deCompactQ(baQ);

            ByteArrayInputStream baTS = new ByteArrayInputStream(binaryTS);
            List<Long> rawTS = Uncompact.deCompactTs(baTS);

            long rowsUncompacted = 0;
            List<TagData> tducList = new ArrayList<TagData>();
            for (int i = 0; i < rawValues.size(); ++i) {
                TagData tdu = new TagData();
                tdu.setUri(uri);
                if (dataType.equals("String")) {
                    tdu.setVs(rawValues.get(i).getVs());
                } else if (dataType.equals("Float")) {
                    tdu.setVd(rawValues.get(i).getVd());
                } else {
                    tdu.setVl(rawValues.get(i).getVl());
                }
                tdu.setQ(rawQ.get(i));
                tdu.setTs(new Timestamp(rawTS.get(i)));
                tducList.add(tdu);
                rowsUncompacted++;
                if (rowsUncompacted % 20000 == 0) {
                    int rowsUpserted = dbService.upsertUncompactedData(unCompactTable, tducList);
                    log.info("Upserted " + rowsUpserted + " records to " + unCompactTable);
                    tducList.clear();
                }
            }
            if (!tducList.isEmpty()) {
                int rowsUpserted = dbService.upsertUncompactedData(unCompactTable, tducList);
                log.info("Upserted " + rowsUpserted + " records to " + unCompactTable);
                tducList.clear();
            }

            if (log.isDebugEnabled()) {
                log.info("rawValues: " + rawValues);
                log.info("rawValues: " + rawValues);
                log.info("rawValues: " + rawValues);
            }

        } catch (SQLException e) {
            throw new RuntimeException("SQLException: " + e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException("IOException:" + e.getMessage());
        } finally {
            DatabaseService.closeConnection();
        }
    }
}
