package org.apache.bookkeeper.metastore.qualify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.metastore.*;
import org.apache.bookkeeper.metastore.testutil.MetastoreScannableTableAsyncToSyncConverter;
import org.apache.bookkeeper.metastore.testutil.ZooKeeperTestUtil;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetastoreScannableTableTest {
	static final Logger logger = LoggerFactory.getLogger(MetastoreScannableTableTest.class);

	@Test
	public void testUsage(String pluginName, String config, String tableName) {
		
		MetastorePlugin plugin = ZooKeeperTestUtil.LoadPlugin(pluginName, config);		
    		
		MetastoreScannableTable myActualTable = plugin.createScannableTable(tableName);
		

		// Catch InterruptedException
		try
		{
			// Use a wrapper for testing sync calls
			MetastoreScannableTableAsyncToSyncConverter myTable =
					new MetastoreScannableTableAsyncToSyncConverter(myActualTable);
		
			// Now add several items for a scan
			String autoKey;
			myTable.addRecord ("1");			
			myTable.addRecord ("2");
			autoKey = myTable.addRecord ("3");
			myTable.addRecord ("4");
			
			System.out.println ("Saved autoKey (used to split) = " + autoKey);


			// Simple usage - scan entire table
			int count = 0;
			{
				ScanResult scan = myTable.scan (null, null);
				while (scan.hasNext ()) {
					count++;
					MetastoreTableItem item = scan.next ();
					System.out.println ("key = " + item.getKey () +
							" value = " + item.getValue ());
				}
			}
			
			System.out.println ("");

			// Scan in two passes, from implied firstKey (null) to "b"
			// (non-inclusive), and from "b" (inclusive) to lastKey (null).
			
			int count2 = 0;
			{
				ScanResult scan = myTable.scan (null, autoKey);
				while (scan.hasNext ()) {
					count2++;
					MetastoreTableItem item = scan.next ();
					System.out.println ("Pass1: key = " + item.getKey () +
							" value = " + item.getValue ());
				}	
			}
			
			{
				ScanResult scan = myTable.scan (autoKey, null);
				while (scan.hasNext ()) {
					count2++;
					MetastoreTableItem item = scan.next ();
					System.out.println ("Pass2: key = " + item.getKey () +
							" value = " + item.getValue ());
				}	
			}
			
			assertEquals ("Scanning from (null, null) has the same count as " +
					"(null, key), (key, null)", count, count2);
			
//		} catch (InterruptedException e) {
//		} catch (TimeoutException e) {
		} catch (Exception e) {
			logger.error(e.toString() + ", stacktrace= " + Arrays.toString(e.getStackTrace()));
		}
		
		plugin.destroyTable (myActualTable);
		plugin.uninit (); 
	}

}
