package org.apache.bookkeeper.metastore.zookeeper.test;


import static org.junit.Assert.*;


import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.metastore.zookeeper.ZooKeeperMetastorePluginUtil;


import org.apache.bookkeeper.metastore.qualify.MetastoreTableTest;
import org.apache.bookkeeper.metastore.qualify.MetastoreScannableTableTest;
import org.apache.bookkeeper.metastore.testutil.ZooKeeperTestUtil;


public class ZooKeeperMetastoreTest {
	
	static final Logger logger =
			LoggerFactory.getLogger(ZooKeeperMetastoreTest.class);
	
	@Test
	public void testUsage() {
		try {
			logger.debug("Test Setup");

			ZooKeeperTestUtil zku = new ZooKeeperTestUtil();
			zku.setup();
			ZooKeeperMetastorePluginUtil zkpu = zku.getZooKeeperPluginUtil();


			// At this point, the server env is initialized, server is up and
			// available, internal server state initialized, and we are
			// connected.
			
			// Create the tables that are expected to exist for the tests
			String tableName = "bk_test_table";
			logger.debug("Creating " + tableName + " (MetastoreTable)");
			zkpu.prepareTable(tableName);
			
			tableName = "bk_test_scan_table";
			logger.debug("Creating " + tableName +
					" (MetastoreScannableTable)");
			zkpu.prepareScannableTable(tableName);
			
			tableName = "bk_test_scan_table_2";
			logger.debug("Creating " + tableName +
					" (MetastoreScannableTable)");
			zkpu.prepareScannableTable(tableName);

			tableName = "bk_test_scan_table_3";
			logger.debug("Creating " + tableName +
					" (MetastoreScannableTable)");
			zkpu.prepareScannableTable(tableName);

			
			// Now, we run the encapsulated test
			final String pluginName =
				"org.apache.bookkeeper.metastore.zookeeper.ZooKeeperMetastorePlugin";
			final String config = "servers=127.0.0.1:2181";
			
			// Basic table test
			tableName = "bk_test_table";
			MetastoreTableTest t1 = new MetastoreTableTest ();
			t1.testUsage(pluginName, config, tableName);
			
			// Do basic scan table test
			tableName = "bk_test_scan_table";
			MetastoreScannableTableTest t2 =
					new MetastoreScannableTableTest ();
			t2.testUsage(pluginName, config, tableName);
			
			// Use a scan table to do an iterator test
			tableName = "bk_test_scan_table_2";
			ZooKeeperIteratorTest t3 = new ZooKeeperIteratorTest ();
			t3.testUsage(pluginName, config, tableName);
			
			// Do the normal metastore table test on a scan table
			tableName = "bk_test_scan_table_3";
			MetastoreTableTest t4 = new MetastoreTableTest ();
			t4.testUsage(pluginName, config, tableName);

			logger.debug("Test Teardown");
			zku.teardown();
		} catch (Exception e) {
			fail("Failed " + e);
		}
	}
}
