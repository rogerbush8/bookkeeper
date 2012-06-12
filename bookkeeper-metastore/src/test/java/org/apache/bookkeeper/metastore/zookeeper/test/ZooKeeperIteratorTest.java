package org.apache.bookkeeper.metastore.zookeeper.test;

import java.util.Vector;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.metastore.*;
import org.apache.bookkeeper.metastore.zookeeper.*;

import org.apache.bookkeeper.metastore.testutil.ZooKeeperTestUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperIteratorTest {
	
	static final Logger logger = LoggerFactory.getLogger(ZooKeeperIteratorTest.class);
	
	@Test
	public void testUsage(String pluginName, String config, String tableName) {
		
		MetastorePlugin plugin = ZooKeeperTestUtil.LoadPlugin(pluginName, config);	
    		
		MetastoreScannableTable myActualTable = plugin.createScannableTable(tableName);
		

		// Catch InterruptedException
		try
		{
			// Now add several items for a scan
			
			final String arr_1[] = {
				"a0/b0",
				"a0/b1/c0", "a0/b1/c1",
				"a0/b2/c2",
				"a0/b3",
				"a0/b4",
				"a0/b5/c3", "a0/b5/c4"
			};
			
			ZooKeeperIterator iter;
			{
				logger.debug("Building tree (in records/) by creating nodes directly");
				
				// We will use the space under "records/" as a scratch space.
				
				ZooKeeperMetastorePluginUtil zkpu =
						((ZooKeeperMetastorePlugin) plugin).getPluginUtil();
				ZooKeeperUtil zku = zkpu.getUtil();
				String rootPath =
						zkpu.getScannableTableRecordPath(myActualTable);
				
				
				for (int i=0 ; i < arr_1.length ; i++) {
					String relPath = arr_1[i];
					zku.ensureRelativePathExists(relPath, rootPath,new byte[0]);
				}
				
				String start = "0000000000";
				String end = "0000000004";
				iter = new ZooKeeperIterator(zkpu.getUtil(), rootPath,
						start, end, 3);
			}
			
			while (iter.hasNext()) {
				ZooKeeperIterator.PathResult result = iter.next();
				Vector<String> slist = result.getChildList();
				System.out.println ("path=" + result.getParentPath() +
						", elements=" + slist.toString());
			}
			
		} catch (InterruptedException e)
		{
		} catch (TimeoutException e)
		{
		} catch (Exception e)
		{
		}
		
		plugin.destroyTable (myActualTable);
		plugin.uninit (); 
	}

}
