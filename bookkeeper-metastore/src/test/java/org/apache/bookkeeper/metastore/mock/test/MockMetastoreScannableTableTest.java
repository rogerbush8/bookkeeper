package org.apache.bookkeeper.metastore.mock.test;

import org.apache.bookkeeper.metastore.qualify.MetastoreTableTest;
import org.junit.Test;

public class MockMetastoreScannableTableTest {

	@Test
	public void testUsage() {

		String pluginName =
				"org.apache.bookkeeper.metastore.mock.MockMetastorePlugin";
			
		String config = "config=value";
		
		String tableName = "mockScanTable";
		MetastoreTableTest test = new MetastoreTableTest ();
		test.testUsage(pluginName, config, tableName);
	}

}
