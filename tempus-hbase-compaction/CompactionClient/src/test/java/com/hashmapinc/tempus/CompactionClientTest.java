/**
 * 
 */
package com.hashmapinc.tempus;

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Ignore;
import org.junit.Test;

import com.hashmapinc.tempus.CompactionClient;
import com.hashmapinc.tempus.DatabaseService;
import com.hashmapinc.tempus.Utils;

/**
 * @author sudeeps
 */

public class CompactionClientTest {

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#getProperties()}.
   */
  @Test
  @Ignore
  public void testGetProperties() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#setProperties(java.util.Properties)}.
   */
  @Test
  @Ignore
  public void testSetProperties() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#getCommandLine()}.
   */
  @Test
  @Ignore
  public void testGetCommandLine() {
    fail("Not yet implemented");
  }

  /**
   * Test method for
   * {@link com.hashmapinc.tempus.CompactionClient#setCommandLine(org.apache.commons.cli.CommandLine)}.
   */
  @Test
  @Ignore
  public void testSetCommandLine() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#getJdbcUpserts()}.
   */
  @Test
  @Ignore
  public void testGetJdbcUpserts() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#setJdbcUpserts(java.lang.Boolean)}.
   */
  @Test
  @Ignore
  public void testSetJdbcUpserts() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#upsertCompactedData()}.
   */
  @Test
  @Ignore
  public void testUpsertCompactedData() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#getUpsertBatchSize()}.
   */
  @Test
  @Ignore
  public void testGetUpsertBatchSize() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#setUpsertBatchSize(long)}.
   */
  @Test
  @Ignore
  public void testSetUpsertBatchSize() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#getRetryMillis()}.
   */
  @Test
  @Ignore
  public void testGetRetryMillis() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#setRetryMillis(long)}.
   */
  @Test
  @Ignore
  public void testSetRetryMillis() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#getTableName()}.
   */
  @Test
  @Ignore
  public void testGetTableName() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#setTableName(java.lang.String)}.
   */
  @Test
  @Ignore
  public void testSetTableName() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#getDbService()}.
   */
  @Test
  @Ignore
  public void testGetDbService() {
    fail("Not yet implemented");
  }

  /**
   * Test method for
   * {@link com.hashmapinc.tempus.CompactionClient#setDbService(com.hashmapinc.tempus.DatabaseService)}.
   */
  @Test
  @Ignore
  public void testSetDbService() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#getHbaseZookeeperUrl()}.
   */
  @Test
  @Ignore
  public void testGetHbaseZookeeperUrl() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#setHbaseZookeeperUrl(java.lang.String)}.
   */
  @Test
  @Ignore
  public void testSetHbaseZookeeperUrl() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#getCompactionWindowTimeInSecs()}.
   */
  @Test
  @Ignore
  public void testGetCompactionWindowTimeInSecs() {
    fail("Not yet implemented");
  }

  /**
   * Test method for
   * {@link com.hashmapinc.tempus.CompactionClient#setCompactionWindowTimeInSecs(java.lang.String)}.
   */
  @Test
  @Ignore
  public void testSetCompactionWindowTimeInSecs() {
    fail("Not yet implemented");
  }

  /**
   * Test method for {@link com.hashmapinc.tempus.CompactionClient#getCompactionWindowTimeInMillis()}.
   */
  @Test
  @Ignore
  public void testGetCompactionWindowTimeInMillis() {
    fail("Not yet implemented");
  }

  /**
   * Test method for
   * {@link com.hashmapinc.tempus.CompactionClient#CompactionClient(org.apache.commons.cli.CommandLine, java.util.Properties)}.
   */
  @Test
  public void testCompactionClientShouldThrowIllegalArgumentException() {
    CommandLine commandLine = null;
    Properties properties = null;
    CompactionClient client = null;
    try {
      //TODO
      client = null;//new CompactionClient(commandLine, properties);
      fail("IllegalArgumentException should have been thrown");
    } catch (IllegalArgumentException e) {
      assertTrue("Caught IllegalArgumentException:" + e.getMessage(),
        (e.getClass() == IllegalArgumentException.class));
    } catch (Exception e) {
      fail("Exception other than IllegalArgumentException is thrown Caught: " + e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.hashmapinc.tempus.CompactionClient#CompactionClient(org.apache.commons.cli.CommandLine, java.util.Properties)}.
   */
  @Test
  public void testCompactionClientShouldThrowParseException() {
    // Load file from resources
    // ClassLoader classLoader = getClass().getClassLoader();
    // String propertiesURL = classLoader.getResource("cpec.properties").getFile();

    CommandLine commandLine = null;
    // missing dashes '--' before props
    String[] args = new String[] { "props", "whatever-file-name" };
    Options options = new Options();
    options.addOption("props", "p", true, "URL of the properties file.");

    try {
      commandLine = new BasicParser().parse(options, args);
      assertNotNull(commandLine);
    } catch (ParseException e) {
      assertTrue("Caught ParseException:" + e.getMessage(), (e.getClass() == ParseException.class));
    } catch (Exception e) {
      fail("Exception other than ParseException is thrown Caught: " + e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.hashmapinc.tempus.CompactionClient#CompactionClient(org.apache.commons.cli.CommandLine, java.util.Properties)}.
   */
  @Test
  public void testCompactionClientShouldThrowConfigurationException() {

    // Load file from resources
    ClassLoader classLoader = getClass().getClassLoader();
    String propertiesURL = classLoader.getResource("cpec-incorrect-format.properties").getFile();

    CommandLine commandLine = null;
    Properties properties = null;

    String[] args = new String[] { "--props", propertiesURL };
    Options options = new Options();
    options.addOption("props", "p", true, "URL of the properties file.");

    try {
      commandLine = new BasicParser().parse(options, args);
      assertNotNull(commandLine);
    } catch (ParseException e) {
      fail("Exception other than ConfigurationException is thrown Caught: " + e.getMessage());
    }

    properties = Utils.loadProperties(propertiesURL);
    assertNotNull(properties);

    try {
      //TODO
      CompactionClient client = null;//new CompactionClient(commandLine, properties);
      fail("ConfigurationException should have been thrown");
    }
    //TODO
    /*catch (ConfigurationException e) {
      assertTrue("Caught ConfigurationException:" + e.getMessage(),
        (e.getClass() == ConfigurationException.class));
    } */catch (Exception e) {
      fail("Exception other than ConfigurationException is thrown Caught: " + e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.hashmapinc.tempus.CompactionClient#CompactionClient(org.apache.commons.cli.CommandLine, java.util.Properties)}.
   */
  @Test
  public void testCompactionClientShouldThrowIllegalArgumentExceptionForNoZkUrl() {

    // Load file from resources
    ClassLoader classLoader = getClass().getClassLoader();
    String propertiesURL = classLoader.getResource("cpec-no-zkurl.properties").getFile();

    CommandLine commandLine = null;
    Properties properties = null;

    String[] args = new String[] { "--props", propertiesURL };
    Options options = new Options();
    options.addOption("props", "p", true, "URL of the properties file.");

    try {
      commandLine = new BasicParser().parse(options, args);
      assertNotNull(commandLine);
    } catch (ParseException e) {
      fail(".Exception other than IllegalArgumentException is thrown Caught: " + e.getMessage());
    }

    try {
      CompactionClient client = null;//new CompactionClient(commandLine, properties);
      fail("IllegalArgumentException should have been thrown");
    } catch (IllegalArgumentException e) {
      assertTrue("Caught IllegalArgumentException:" + e.getMessage(),
        (e.getClass() == IllegalArgumentException.class));
    } catch (Exception e) {
      fail(".Exception other than IllegalArgumentException is thrown Caught: " + e.getMessage());
    }
  }

  /**
   * Test method for
   * {@link com.hashmapinc.tempus.CompactionClient#CompactionClient(org.apache.commons.cli.CommandLine, java.util.Properties)}.
   * @throws ParseException
   */
  @Test
  public void testCompactionClientShouldPass() {
    // Load file from resources
    ClassLoader classLoader = getClass().getClassLoader();
    String propertiesURL = classLoader.getResource("cpec.properties").getFile();

    String[] args = new String[] { "--props", propertiesURL };
    Options options = new Options();
    options.addOption("props", "p", true, "URL of the properties file.");

    CommandLine commandLine = null;
    Properties properties = null;

    try {
      commandLine = new BasicParser().parse(options, args);
      assertNotNull(commandLine);
    } catch (ParseException e) {
      fail(".Exception other than IllegalArgumentException is thrown Caught: " + e.getMessage());
    }

    properties = Utils.loadProperties(propertiesURL);
    assertNotNull(properties);

    try {
      //TODO
      CompactionClient client = null;//new CompactionClient(commandLine, properties);
      assertNotNull(client);
    } catch (Exception e) {
      fail(".Exception  " + e.getClass() + " is thrown. " + e.getMessage());
    }

  }

  /**
   * Test method for
   * {@link com.hashmapinc.tempus.CompactionClient#compactWindowedData(java.lang.String, long, long)}.
   * Failing due to foll Exception [class java.lang.SecurityException is thrown. class
   * "javax.servlet.FilterRegistration"'s signer information does not match signer information of
   * other classes in the same package]
   */
  @Test
  @Ignore
  public void testCompactWindowedData() {
    ClassLoader classLoader = getClass().getClassLoader();
    String propertiesURL = classLoader.getResource("cpec.properties").getFile();

    String[] args = new String[] { "--props", propertiesURL };
    Options options = new Options();
    options.addOption("props", "p", true, "URL of the properties file.");

    CommandLine commandLine = null;
    Properties properties = null;

    try {
      commandLine = new BasicParser().parse(options, args);
      assertNotNull(commandLine);
    } catch (ParseException e) {
      fail(".Exception other than IllegalArgumentException is thrown Caught: " + e.getMessage());
    }

    properties = Utils.loadProperties(propertiesURL);
    assertNotNull(properties);
/*
    try {
      CompactionClient client = new CompactionClient(commandLine, properties);
      DatabaseService dbService;
      dbService = new DatabaseService(client.getHbaseZookeeperUrl());
      dbService.openConnection();
      dbService.setCompactionStatusTable(Utils.readProperty(properties,
        CompactionClient.PHOENIX_COMPACTION_STATUS_TABLE_NAME_PROPERTY));
      dbService.setPointsTagTable(client.getTableName());
      dbService.setCompactionTable(Utils.readProperty(properties,
        CompactionClient.PHOENIX_TAG_DATA_COMPACT_TABLE_NAME_PROPERTY));
      client.setDbService(dbService);
      assertEquals(0, client.compactWindowedData("point-Tag", 0, System.currentTimeMillis()));
    } catch (Exception e) {
      fail(".Exception  " + e.getClass() + " is thrown. " + e.getMessage());
    }
    */
  }

}
