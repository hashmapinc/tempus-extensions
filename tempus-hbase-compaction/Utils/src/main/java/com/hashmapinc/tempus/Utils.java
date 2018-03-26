package com.hashmapinc.tempus;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.sql.Date;
import java.util.Calendar;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;

public class Utils {
  final static Logger log = Logger.getLogger(Utils.class);

  public Utils() {
    // TODO Auto-generated constructor stub
  }

  public static String readPidFile(String runFile) {
    String line = null;
    File pidFile = new File(runFile);
    try (BufferedReader reader = Files.newBufferedReader(pidFile.toPath())) {
      while ((line = reader.readLine()) != null) {
        log.info("PID from file : " + pidFile + "[" + line +"]");
        break;
      }
    } catch (IOException x) {
      log.error("IOException: %s%n", x);
    }
    return line;
  }

  public static String writePidFile(String pid, String runFile) {
    String line = null;
    File pidFile = new File(runFile);
    try (BufferedWriter writer = Files.newBufferedWriter(pidFile.toPath())) {
      writer.write(pid, 0, pid.length());
    } catch (IOException x) {
      log.error("IOException: %s%n", x);
    }
    return line;
  }

  public static void deletePidFile(String runFile) {
    File pidFile = new File(runFile);
    try {
      Files.delete(pidFile.toPath());
    } catch (NoSuchFileException x) {
      log.error(pidFile.toPath() + ": no such" + " file or directory.");
    } catch (IOException x) {
      // File permission problems are caught here.
      log.error(x);
    }
  }

  public static Boolean isPreviousCompactionRunning(String runFile) {
    File pidFile = new File(runFile);
    if (Files.notExists(pidFile.toPath())) {
      return false;

    } else {
      String pid = readPidFile(runFile);
      if (pid == null) {
        log.info("null pid");
        // PID is null, but file exists. Inconsistent
        if (Files.exists(pidFile.toPath())) {
          log.info("Previous instance of compaction created pid file " + pidFile
              + " with null pid. Check error logs and remove the pid file if no issues.");
          return true;
        } else return false;
      } else {
        // PID is not null. Check in /proc is process is still running
        // If not delete crun file and run this instance
        File sysPidFile = new File("/proc/" + pid);
        if (Files.exists(sysPidFile.toPath())) {
          log.info("Previous instance of compaction is still running as pid file " + pidFile
              + " exists with pid:" + pid);
          return true;
        } else {
          log.info("Previous instance of compaction is not running but pid file " + pidFile
              + " exists with pid:" + pid + ". Deleting...");
          deletePidFile(runFile);
          return false;
        }
      }
    }
  }

  public static String getValueFromArgs(CommandLine commandLine, String longOpt, String shortOpt) {
    if (commandLine.hasOption(longOpt)) {
      return commandLine.getOptionValue(longOpt);
    } else if (commandLine.hasOption(shortOpt)) {
      return commandLine.getOptionValue(shortOpt);
    } else {
      log.warn("Property " + shortOpt + "/" + longOpt + " undefined.");
    }
    return null;
  }

  public static String readProperty(Properties properties, String property) {
    String value = properties.getProperty(property);
    if (value == null) {
      log.error("property not defined: " + property);
      System.exit(-1);
    }
    return value;
  }

  
  public static String readProperty(Properties properties, String property, Boolean exit) {
    String value = properties.getProperty(property);
    if (value == null) {
      log.error("property not defined: " + property);
      if (exit) System.exit(-1);
    }
    return value;
  }

  public static String readProperty(Properties properties, String property,
      String defaultProperty) {

    String value = readOptionalProperty(properties, property).orElse((defaultProperty == null) ?
            "" : defaultProperty);
    //log.info("Optional read value for " + property + " is " + value + "; default property is :- " + defaultProperty);
    return value;
  }

  private static Optional<String> readOptionalProperty(Properties properties, String property) {
    Optional<String> value = Optional.ofNullable(properties.getProperty(property));
    return value;
  }

  public static Properties loadProperties(String propertiesURL) {
    Properties properties = new Properties();
    FileReader reader = null;
    try {
      reader = new FileReader(propertiesURL);
      properties.load(reader);
      return properties;
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception e) {
        }
      }
    }
    return null;
  }

  public static long convertToUTC(long ts) {
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

  public static void validateHbaseZookeeperUrl(String hbaseZookeeperUrl)
      throws ConfigurationException {
    if (!hbaseZookeeperUrl.contains(":") || !hbaseZookeeperUrl.contains("/")) {
      throw new ConfigurationException(
          "HBase Zookeper Url doesn't look right: " + hbaseZookeeperUrl);
    }
  }

  //For Long URI's
  public static byte[] createScanRow(long uri, Long tsMillis, Integer tsNanos) {
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

  public static byte[] createScanStartRow(long startURI, long origStartTs) {
    log.debug("startURI[" + startURI + "]; startTs[" + origStartTs + "];");

    byte[] startRow = null;
    if (origStartTs == 0) {
      Long hbUri = -(Long.MIN_VALUE - startURI);
      byte[] bytesStartURI = Bytes.toBytes(hbUri);
      startRow = new byte[bytesStartURI.length];
      Bytes.putBytes(startRow, 0, bytesStartURI, 0, bytesStartURI.length);
    } else {
      return createScanRow(startURI, origStartTs, null);
    }
    return startRow;
  }

  public static byte[] createScanStopRow(long stopURI, long origStopTs) {
    return createScanRow(stopURI, origStopTs, null);
  }

  public static byte[] createPhoenixRow(long uri, long ts){
    //Phoenix
    byte[] phoenixUri = PLong.INSTANCE.toBytes(uri);
    byte[] phoenixTs = PLong.INSTANCE.toBytes(ts);
    byte[] retRow = Bytes.add(phoenixUri, phoenixTs);
    return retRow;

  }

  //For String URI's
  public static byte[] createScanRow(String pointTag, Long tsMillis, Integer tsNanos) {
    //log.debug("pointTag[" + pointTag + "]; tsMillis[" + tsMillis + "]; tsNanos[" + tsNanos +
    // "];");
    Long hbMillisTs = -(Long.MIN_VALUE - tsMillis);
    byte[] bytesPointTag = Bytes.toBytes(pointTag);
    byte[] bytesTsMillis = Bytes.toBytes(hbMillisTs);
    byte[] bytesTsNanos = null;

    byte[] hbRowKey = null;
    if (tsNanos != null) {
      Integer hbNanosTs = tsNanos;
      bytesTsNanos = Bytes.toBytes(hbNanosTs);
      hbRowKey = new byte[bytesPointTag.length + bytesTsMillis.length + bytesTsNanos.length + 1];
    } else {
      hbRowKey = new byte[bytesPointTag.length + bytesTsMillis.length + 1];
    }
    int incrOffset = Bytes.putBytes(hbRowKey, 0, bytesPointTag, 0, bytesPointTag.length);
    incrOffset = Bytes.putByte(hbRowKey, incrOffset, (byte) 0);
    incrOffset = Bytes.putBytes(hbRowKey, incrOffset, bytesTsMillis, 0, bytesTsMillis.length);
    if (tsNanos != null) {
      incrOffset = Bytes.putBytes(hbRowKey, incrOffset, bytesTsNanos, 0, bytesTsNanos.length);
    }
    return hbRowKey;
  }

  public static byte[] createScanStartRow(String startPointTag, Long origStartTs) {
    //log.debug("startPointTag[" + startPointTag + "]; startTs[" + origStartTs + "];");
    byte[] bytesStartPointTag = Bytes.toBytes(startPointTag);
    byte[] startRow = null;
    if (origStartTs == 0) {
      startRow = new byte[bytesStartPointTag.length];
      Bytes.putBytes(startRow, 0, bytesStartPointTag, 0, bytesStartPointTag.length);
    } else {
      return createScanRow(startPointTag, origStartTs, null);
    }
    return startRow;
  }

  public static byte[] createScanStopRow(String stopPointTag, Long origStopTs) {
    return createScanRow(stopPointTag, origStopTs, null);
  }
}
