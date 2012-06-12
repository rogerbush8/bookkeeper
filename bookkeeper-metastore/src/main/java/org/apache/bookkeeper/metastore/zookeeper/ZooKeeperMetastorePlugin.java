package org.apache.bookkeeper.metastore.zookeeper;


import org.apache.bookkeeper.metastore.*;
import org.apache.zookeeper.ZooKeeper;

import java.util.regex.*;

//import org.apache.bookkeeper.metastore.zookeeper.ZooKeeperUtil;

public class ZooKeeperMetastorePlugin implements MetastorePlugin {
	
	private String servers;
	private ZooKeeperMetastorePluginUtil zkpu = null;
	static final private String zooKeeperMetastorePath = "/bookkeeper/metastore/zookeeper";
	
	
	public static String getTablePath(String tableName) {
		return zooKeeperMetastorePath + "/" + tableName;
	}
	
	public ZooKeeperMetastorePlugin () {
	}
	
	public String getName () {
		return "ZooKeeperMetadataPlugin";
	}
	
	public String getVersion () {
		return "1.0";
	}
	
	public void init(String config) throws MetastorePluginException {

		final Pattern wsRegex = Pattern.compile("\\s+");
		String[] pairs = wsRegex.split(config);
		for (int i = 0; i < pairs.length; i++) {
			String[] nv = pairs[i].split("=");
			String n = nv[0];
			String v = nv[1];
			if (n.equals("servers")) {
				// TODO - we should really check this string for correctness
				this.servers = v;
			} else {
				throw new MetastorePluginException (
						"Unrecognized configuration parameter=" + n
								+ ", with value=" + v);
			}
		}
		
		executeInit ();
	}

		
	private void executeInit () throws MetastorePluginException {
		
		// Check that all necessary parameters were set
		
		if (servers == null) {
			throw new MetastorePluginException ("'servers' configuration parameter not specified " +
						"(e.g. 'servers=host1:port1,host2:port2')");
		}
		
		zkpu = new ZooKeeperMetastorePluginUtil(this.servers);			
		
		if ( ! zkpu.testConnect())
			throw new MetastorePluginException ("Couldn't connect to servers=" + this.servers);
		
		byte [] data = ZooKeeperMetastorePluginUtil.encode ("test");
		if (data == null)
			throw new MetastorePluginException ("UTF-8 encode failed.");
		
		String temp = ZooKeeperMetastorePluginUtil.decode(data);
		if (temp == null)
			throw new MetastorePluginException ("UTF-8 decode failed");
		
		if ( ! temp.equals("test"))
			throw new MetastorePluginException ("UTF-8 encode/decode sequence failed");
	}
	
	public void uninit () {
		try {
			zkpu.close();
		} catch (InterruptedException e) {
		}
		zkpu = null;
	}
	
	public MetastoreTable createTable (String name) {
		// TODO - we should validate that the table node exists, and if
		// not, throw an exception.
		return new ZooKeeperMetastoreTable (this, name);
	}
	
	public void destroyTable (MetastoreTable table) {
	}
	
	public MetastoreScannableTable createScannableTable (String name) {
		return new ZooKeeperMetastoreScannableTable (this, name);
	}
	
	public void destroyScannableTable (MetastoreScannableTable table) {
	}
	
	String getServersString() {
		return servers;
	}
	
	public ZooKeeperMetastorePluginUtil getPluginUtil() {
		return zkpu;
	}
	
	public ZooKeeperUtil getUtil() {
		return getPluginUtil().getUtil();
	}
	
	public ZooKeeper getClient() {
		return getUtil().getClient();
	}
	
}
