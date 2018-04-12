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
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDataTypeFactory;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PIntegerArray;
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
}
