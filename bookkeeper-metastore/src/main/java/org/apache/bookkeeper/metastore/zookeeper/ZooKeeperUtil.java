package org.apache.bookkeeper.metastore.zookeeper;

import org.apache.bookkeeper.metastore.testutil.MetastoreCompletion;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author rbush
 */
public class ZooKeeperUtil {
	static final Logger logger = LoggerFactory.getLogger(ZooKeeperUtil.class);

	private String servers;
	private ZooKeeper zkc;
	
	public ZooKeeperUtil(String servers) {
		this.servers = servers;
	}
	
	public ZooKeeper getClient() {
		if (zkc == null) {
			logger.debug("Instantiate ZK Client");
			zkc = ZooKeeperUtil.getConnectedZooKeeperClient
					(servers, 10000);
		}
		return zkc;
	}
	
	public void close() throws InterruptedException {
		if (zkc != null) {
			zkc.close();
			zkc = null;
		}
	}
	
	public boolean testConnect() {
		getClient();
		return zkc != null ? true : false;
	}
	
	public static ZooKeeper getConnectedZooKeeperClient (String servers, int waitMilliseconds)
	{
		final MetastoreCompletion<Boolean> cb = new MetastoreCompletion<Boolean>();
		ZooKeeper zkc;
		try {
			zkc = new ZooKeeper(servers, waitMilliseconds, new Watcher() {
				public void process(WatchedEvent event) {
					// handle session disconnects and expires
					if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected))
						cb.complete(0, null, Boolean.TRUE);
				}
			});
			
			cb.await();

		} catch (Exception e) {
			return null;
		}

		return zkc;
	}
	
	public void createPersistentNode(String path) throws Exception {
		createPersistentNode(path, new byte[0]);
	}

	public void createPersistentNode(String path, byte[] data) throws Exception {
		getClient().create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	
	 
	
	/**
	 * Will create or set the node with the data, creating intermediate path elements if required.
	 * @param path - ZK path.  Path to create.  After call, this path is guaranteed to exist, with
	 *        data set at rightmost path element.
	 * @param assumedExistsPath - first part of path that should not be created (assume it already exists)
	 * @param data - ZK data (set at path node)
	 * @throws Exception
	 */
	public void ensurePathExists(String path, String assumedExistsPath, byte[] data) throws Exception {
		// Attempt to create the node..if fail, then probe forward
		try {
			createPersistentNode(path, data);
		} catch (KeeperException e) {
			if (e.code() == Code.NODEEXISTS) {
				// Node exists, attempt to overwrite data.  May throw
				getClient().setData(path, data, -1);
			} else if (e.code() == Code.NONODE) {
				buildPath(path, assumedExistsPath, data);
			}
		}
	}
	
	public void ensureRelativePathExists(String relPath, String assumedExistsPath, byte[] data) throws Exception {
		ensurePathExists (assumedExistsPath + "/" + relPath, assumedExistsPath, data);
	}
	
	/**
	 * 	Creates a path of persistent nodes starting from an existing assumedExistsPath.
	 *  Does this by calling createPersistentNode for each element on the path beyond
	 *  the assumedExistsPath, from left to right.  If there is an error with the arguments
	 *  (e.g. invalid assumedExistsPath), the function terminates without altering ZK state.
	 *  If there is a dynamic error of some type, partial ZK state will be altered (some
	 *  portion of the path).  Function is "restartable" - if a dynamic failure occurs,
	 *  can be called again with same arguments, and it will pick up where it left off
	 *  and finish successfully (can be restarted any number of times to respond to
	 *  multiple dynamic failures).
	 * @param path - ZK path.  Path to create.  After call, this path is guaranteed to exist, with
	 *        data set at rightmost path element.
	 * @param assumedExistsPath - first part of path that should not be created (assume it already exists)
	 * @param data - ZK data (set at path node)
	 * @throws Exception
	 */
	public void buildPath(String path, String assumedExistsPath, byte[] data) throws Exception {
		int idx = path.indexOf(assumedExistsPath);
		if (idx < 0)
			throw new Exception("assumedExistsPath is not found in path.  " +
					"It must be a valid starting subpath of path");
		if (idx > 0)
			throw new Exception("assumedExistsPath was found in path, but not at the start.  " +
					"It must be a valid starting subpath of path");
		
		String relativeBuildPath = path.substring(assumedExistsPath.length());
		
		// If relativeBuildPath has a leading "/" then remove it.
		idx = relativeBuildPath.indexOf("/");
		if (idx == 0)
			relativeBuildPath = relativeBuildPath.substring(1);	
		
		String[] pathElements = relativeBuildPath.split("/");
		
		// Validate individual path elements
		
		for (int i=0 ; i < pathElements.length ; i++)
		{
			String el = pathElements[i];
			if (el.length() == 0)
				throw new Exception("path element " + i + " is empty");
			// TODO - should check for funny characters
		}
		
		String currentPath = assumedExistsPath;
		for (int i=0 ; i < pathElements.length ; i++)
		{
			String el = pathElements[i];
			currentPath = currentPath + "/" + el;
			
			boolean last = i == pathElements.length - 1;
			try {
				if (last)
					createPersistentNode(currentPath, data);
				else
					createPersistentNode(currentPath);
			} catch (KeeperException e) {
				if (e.code() == Code.NODEEXISTS) {
					if (last) {
						try {
							getClient().setData(currentPath, data, -1);
						} catch (KeeperException e2) {
							if (e2.code() == Code.NONODE)
								throw new Exception("Collision, someone deleted the node.  Pause, retry.");
						}
					}
					// Keep going, will terminate if last
				}
			}
		}
	}

}
