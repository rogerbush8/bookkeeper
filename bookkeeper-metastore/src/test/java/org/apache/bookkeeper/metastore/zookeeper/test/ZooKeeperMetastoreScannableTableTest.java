package org.apache.bookkeeper.metastore.zookeeper.test;

import org.apache.bookkeeper.metastore.qualify.MetastoreTableTest;
import org.junit.Test;

public class ZooKeeperMetastoreScannableTableTest {

	@Test
	public void testUsage(String tableName) {

		String pluginName =
				"org.apache.bookkeeper.metastore.zookeeper.ZooKeeperMetastorePlugin";
			
		String config = "config=value";
			
		MetastoreTableTest test = new MetastoreTableTest ();
		test.testUsage(pluginName, config, tableName);
	}

}
