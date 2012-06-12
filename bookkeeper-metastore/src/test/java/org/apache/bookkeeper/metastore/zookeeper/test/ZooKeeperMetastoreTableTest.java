package org.apache.bookkeeper.metastore.zookeeper.test;


import org.apache.bookkeeper.metastore.qualify.MetastoreTableTest;
import org.junit.Test;

public class ZooKeeperMetastoreTableTest {

	@Test
	public void testUsage (String tableName) {

		String pluginName =
				"org.apache.bookkeeper.metastore.zookeeper.ZooKeeperMetastorePlugin";
		
		String config = "servers=127.0.0.1:2181";
		
		MetastoreTableTest test = new MetastoreTableTest ();
		test.testUsage(pluginName, config, tableName);
	}

}
