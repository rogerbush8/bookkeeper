package org.apache.bookkeeper.metastore.zookeeper;


import java.util.TreeMap;

import org.apache.bookkeeper.metastore.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;

public class ZooKeeperMetastoreTable implements MetastoreTable {
	
	private ZooKeeperMetastorePlugin plugin;
	private String name;
	private ZooKeeper zkc;
	
	TreeMap<String, String> map = null;
	
	ZooKeeperMetastorePlugin getPlugin() {
		return plugin;	
	}
	
	public String tablePath() {
		return ZooKeeperMetastorePlugin.getTablePath(this.name);
	}
	
	public String getRecordPath() {
		return tablePath() + "/records";
	}
	
	public String getKeyPath(String relativeKeyPath) {
		return getRecordPath() + "/" + relativeKeyPath;
	}

	public ZooKeeperMetastoreTable (ZooKeeperMetastorePlugin plugin, String name) {
		this.plugin = plugin;
		this.map = new TreeMap<String,String> ();
		this.name = name;
	}
	
	public String getName () {
		return this.name;
	}
	
	public void get (MetastoreCallback<String> cb, String key) {
		String path = getKeyPath(key);
		zkc = plugin.getPluginUtil().getUtil().getClient();
		Stat stat = new Stat();
		int rc = 0;
		String errorDescription = null;
		String value = null;
		byte [] data;
		
		// TODO - replace with async
		try {
			data = zkc.getData(path, false, stat);
			value = ZooKeeperMetastorePluginUtil.decode(data);
		} catch (KeeperException e) {
			if (e.code() == Code.NONODE) {
				// No op.  Will return null.
			}
			else
			{
				rc = 2;
				errorDescription = e.toString();
			}
		} catch (Exception e) {
			rc = 1;
			errorDescription = e.toString();
		}
		cb.complete (rc, errorDescription, value);
	}
	
	public void put (MetastoreCallback<String> cb, String key, String value) {
		String path = getKeyPath(key);
		zkc = plugin.getPluginUtil().getUtil().getClient();
		int rc = 0;
		String errorDescription = null;		
		byte [] valueBytes = ZooKeeperMetastorePluginUtil.encode(value);

		
		// TODO - replace with async
		try {
			// TODO - is there a version of create that will "overwrite" the data?
			// this would be more analogous to put.
			zkc.create(path, valueBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			if (e.code() == Code.NODEEXISTS) {
				// put is supposed to either set a value or "auto-create" the node.
				// There is no analog to this as an atomic operation in ZooKeeper, so
				// we will now try to set the data.  There is the possibility that
				// someone deletes the node as we try to set it.  In that case we
				// will fail, and the application can treat this as a retry.
				// In general, this means that having multiple collisions on a
				// key in an attempt to put or remove is not advised (maybe CAS?)
				
				try {
					int versionOverwrite = -1;
					zkc.setData(path, valueBytes, versionOverwrite);
				} catch (KeeperException e2) {
					rc = 3;
					errorDescription = "Node exists, attempt to set data failed. " + e2.toString();
				} catch (Exception e2) {
					rc = 4;
					errorDescription = "Node exists, attempt to set data failed. " + e2.toString();					
				}
			}
			else
			{
				rc = 2;
				errorDescription = e.toString();
			}
		} catch (Exception e) {
			rc = 1;
			errorDescription = e.toString();
		}
		cb.complete (rc, errorDescription, value);
	}
	
	public void remove (MetastoreCallback<String> cb, String key) {
		String path = getKeyPath(key);
		zkc = plugin.getPluginUtil().getUtil().getClient();
		int rc = 0;
		String errorDescription = null;

		// TODO - replace with async
		try {
			int versionOverwrite = -1;
			zkc.delete(path, versionOverwrite);
		} catch (KeeperException e) {
			if (e.code() == Code.NONODE)
			{
				// OK to delete a non-existent node
			}
			else
			{
				rc = 2;
				errorDescription = e.toString();
			}
		} catch (Exception e) {
			rc = 1;
			errorDescription = e.toString();
		}
		// String v = mockGet (key);
		cb.complete (rc, errorDescription, key);
	}

	// TODO - this is not converted yet.
	public void compareAndPut (MetastoreCallback<Boolean> cb, String key, String oldValue, String newValue) {
		boolean success = mockCompareAndPut (key, oldValue, newValue);
		int rc = 0;
		cb.complete (rc, null, new Boolean (success));
	}
		
	private boolean mockCompareAndPut (String key, String oldValue, String newValue) {
		String v = map.get (key);
		
		if ( ! v.equals(oldValue))
			return false;
		
		map.put (key, newValue);
		
		return true;
	}
}
