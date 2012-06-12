package org.apache.bookkeeper.metastore.qualify;
/**
 * Copyright 2012 Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */
import static org.junit.Assert.*;

import org.apache.bookkeeper.metastore.*;
import org.apache.bookkeeper.metastore.testutil.MetastoreTableAsyncToSyncConverter;
import org.apache.bookkeeper.metastore.testutil.ZooKeeperTestUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;


public class MetastoreTableTest {
	
    static final Logger logger = LoggerFactory.getLogger (MetastoreTableTest.class);

	public void testUsage(String pluginName, String config, String tableName) {
				
		MetastorePlugin plugin = ZooKeeperTestUtil.LoadPlugin(pluginName, config);		
		
		logger.debug("Creating table=" + tableName);
		MetastoreTable myActualTable = plugin.createTable(tableName);
		
		
		// ** Test get, put, remove
		
		String k = "a";
		String v;
		
		// Catch InterruptedException
		try
		{
			// Use a wrapper for testing sync calls
			MetastoreTableAsyncToSyncConverter myTable =
					new MetastoreTableAsyncToSyncConverter(myActualTable);
			
			// Make sure key isn't there
			myTable.remove(k);
			
			// Should be OK to delete a non-existent element
			myTable.remove(k);
			
			
			v = myTable.get(k);
			assertNull("Doing get on a non-existent element returns null", v);
		
		
			// put is unconditional overwrite
		
			myTable.put(k, "12");
			
		
			v = myTable.get(k);	
			assertNotNull("Get key we just put", v);
			assertEquals("Put stores value", v, "12");
			
			myTable.put(k, "13");
			v = myTable.get(k);
			assertEquals("Put overwrites existing value", v, "13");

			myTable.remove(k);
			v = myTable.get(k);	
			assertNull("Remove key we just put", v);
		
		
//			// TODO - We need the older version of compare and put (putVersioned).
//			//        For now, disabling these tests
//			// ** Test compareAndPut
//			
//			String oldValue = "7";
//			myTable.put (k, oldValue);
//		
//			// Let's try a failure
//			String newValue = "8";
//			String notOldValue = newValue;
//			boolean ok = myTable.compareAndPut (k, notOldValue, newValue);
//			assertFalse ("compareAndPut returns false when stored value is different from oldValue", ok);
//		
//			// Should still have oldValue in it
//			v = myTable.get (k);
//			assertEquals ("compareAndPut shouldn't change value if it returns false", oldValue, v);
//			
//			// Now successfully store value
//			ok = myTable.compareAndPut(k, oldValue, newValue);
//			assertTrue ("compareAndPut returns true when oldValue == stored value", ok);
//			v = myTable.get (k);
//			assertEquals ("compareAndPut updates value when it returns true", newValue, v);

			
		} catch (InterruptedException e)
		{
		} catch (TimeoutException e)
		{
		} catch (Exception e)
		{
		}
		

		plugin.destroyTable(myActualTable);
		plugin.uninit(); 
	}

}
