package org.apache.bookkeeper.metastore.mock.test;

import org.apache.bookkeeper.metastore.qualify.MetastoreTableTest;
import org.junit.Test;

public class MockMetastoreTableTest {

	@Test
	public void testUsage() {

		String pluginName =
				"org.apache.bookkeeper.metastore.mock.MockMetastorePlugin";
		
		String config = "config=value";
		
		String tableName = "mockTable";
		MetastoreTableTest test = new MetastoreTableTest ();
		test.testUsage(pluginName, config, tableName);
	}

}
