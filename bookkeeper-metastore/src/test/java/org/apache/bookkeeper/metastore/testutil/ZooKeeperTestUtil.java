package org.apache.bookkeeper.metastore.testutil;

/*
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*/


import java.io.File;
import java.net.InetSocketAddress;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;


import static org.junit.Assert.*;

import org.apache.bookkeeper.metastore.MetastorePlugin;
import org.apache.bookkeeper.metastore.MetastorePluginException;
import org.apache.bookkeeper.metastore.PluginLoader;
import org.apache.bookkeeper.metastore.zookeeper.ZooKeeperMetastorePluginUtil;

public class ZooKeeperTestUtil {
	static final Logger logger = LoggerFactory.getLogger(ZooKeeperTestUtil.class);

	// ZooKeeper related variables
	protected static Integer ZooKeeperDefaultPort = 2181;
	private final InetSocketAddress zkaddr;

	protected ZooKeeperServer zks;
	
	protected ZooKeeperMetastorePluginUtil zkpu;
	
	protected NIOServerCnxnFactory serverFactory;
	protected File ZkTmpDir;
	private final String connectString;

	public ZooKeeperTestUtil () {
		zkaddr = new InetSocketAddress(ZooKeeperDefaultPort);
		connectString = "localhost:" + ZooKeeperDefaultPort;
	}
	
	public ZooKeeperMetastorePluginUtil getZooKeeperPluginUtil() {
		if (zkpu == null) {
			logger.debug ("Instantiate ZK Plugin Util");
			zkpu = new ZooKeeperMetastorePluginUtil (getZooKeeperConnectString());
		}
		return zkpu;
	}

	public String getZooKeeperConnectString () {
		return connectString;
	}

	public void initServerEnvironment() throws Exception {
		logger.debug("Initializing ZooKeeper Environment (Test)");
		ClientBase.setupTestEnv();
		ZkTmpDir = File.createTempFile("zookeeper", "test");
		ZkTmpDir.delete();
		ZkTmpDir.mkdir();
	}

	public void startServer() throws Exception {
		// create a ZooKeeper server(dataDir, dataLogDir, port)
		logger.debug ("Starting ZooKeeper Server (Test)");

		zks = new ZooKeeperServer (ZkTmpDir, ZkTmpDir, ZooKeeperDefaultPort);
		serverFactory = new NIOServerCnxnFactory ();
		serverFactory.configure (zkaddr, 100);
		serverFactory.startup (zks);

		boolean b = ClientBase.waitForServerUp (getZooKeeperConnectString (),
				ClientBase.CONNECTION_TIMEOUT);

		logger.debug("Server up: " + b);

		if ( ! getZooKeeperPluginUtil().testConnect())
			fail("Couldn't connect to zk servers");
	}

	// TODO - we should read the path from zookeeper plugin, and use util
	// to create a path.
	public void initServerState() throws Exception {
		 getZooKeeperPluginUtil().preparePluginState();
	}
	
	public void cleanServerState() throws Exception {
		// TODO
	}

	public void stopServer() throws Exception {
		if (zkpu != null) {
			zkpu.close();
		}

		// shutdown ZK server
		if (serverFactory != null) {
			serverFactory.shutdown();
			assertTrue("waiting for server down", ClientBase.waitForServerDown(
					getZooKeeperConnectString(), ClientBase.CONNECTION_TIMEOUT));
		}
		if (zks != null) {
			zks.getTxnLogFactory().close();
		}
	}
	
	public void cleanServerEnvironment() throws Exception {
		// ServerStats.unregister();
		FileUtils.deleteDirectory(ZkTmpDir);
	}
	
	public void setup() throws Exception {
		initServerEnvironment();
		startServer();
		initServerState();		
	}
	
	public void teardown() throws Exception {
		cleanServerState();
		stopServer();
		cleanServerEnvironment();		
	}
	
	static public MetastorePlugin LoadPlugin (String pluginName, String config) {
		PluginLoader loader = new PluginLoader();

		MetastorePlugin plugin = null;
		try
		{
			plugin = loader.loadPlugin(pluginName);
			plugin.init(config);
		} catch (MetastorePluginException e) {
			fail("Plugin init failure");
		} catch (Exception e) {
			fail("Plugin failure " + e);
		}
		return plugin;
	}

	
}