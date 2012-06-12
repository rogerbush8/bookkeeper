package org.apache.bookkeeper.metastore.zookeeper;

import java.io.UnsupportedEncodingException;

import org.apache.bookkeeper.metastore.MetastoreScannableTable;

public class ZooKeeperMetastorePluginUtil {
	
	private ZooKeeperUtil zku;
	
	static byte [] encode (String value) {
		// The only reason this would fail is if the "UTF-8" encoder/decoder
		// wasn't available.  So testing this function a single time will
		// validate it will always work.
		
		try {
			return value.getBytes("UTF-8");
		} catch (Exception e) {
			return null;
		}
	}
	
	static String decode(byte[] data) {
		try {
			return new String(data, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return null;
		}
	}
		
	public ZooKeeperMetastorePluginUtil(String servers) {
		zku = new ZooKeeperUtil(servers);
	}
	
	public boolean testConnect() {
		return zku.testConnect();
	}
	
	public void close() throws InterruptedException {
		zku.close();
	}
	
	public ZooKeeperUtil getUtil() {
		return zku;
	}
	
	public void preparePluginState() throws Exception {
		zku.createPersistentNode("/bookkeeper");
		zku.createPersistentNode("/bookkeeper/metastore");
		zku.createPersistentNode("/bookkeeper/metastore/zookeeper");
	}
	
	public void prepareTable(String tableName) throws Exception {
		String path = ZooKeeperMetastorePlugin.getTablePath(tableName);
		zku.createPersistentNode(path);	
		zku.createPersistentNode(path + "/records");	
	}
	
	public void prepareScannableTable(String tableName) throws Exception {
		String path = ZooKeeperMetastorePlugin.getTablePath(tableName);
		zku.createPersistentNode(path);	
		zku.createPersistentNode(path + "/records");	
		zku.createPersistentNode(path + "/counter");
	}
	
	public String getScannableTableRecordPath(MetastoreScannableTable table) throws Exception {
		ZooKeeperMetastoreScannableTable table2 = (ZooKeeperMetastoreScannableTable) table;
		String path = ZooKeeperMetastorePlugin.getTablePath(table2.getName());
		return path + "/records";
	}
	
}
