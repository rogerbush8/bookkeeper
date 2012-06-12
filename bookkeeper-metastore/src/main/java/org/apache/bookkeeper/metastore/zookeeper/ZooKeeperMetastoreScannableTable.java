package org.apache.bookkeeper.metastore.zookeeper;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;


import org.apache.bookkeeper.metastore.*;
import org.apache.bookkeeper.metastore.testutil.MetastoreCompletion;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;


/**
 * @author rbush
 */

public class ZooKeeperMetastoreScannableTable extends ZooKeeperMetastoreTable
	implements MetastoreScannableTable {
	
	public ZooKeeperMetastoreScannableTable(ZooKeeperMetastorePlugin plugin,
			String name) {
		super (plugin, name);
	}
	
	private String getNumberKeyPath() {
		return tablePath() + "/counter";
	}
	
	private String numberKeyToNumberKeyRelativePath (String numberKey) {
		// e.g. "0010020003" => "001/002/0003"

		String p1 = numberKey.substring(0, 3);
		String p2 = numberKey.substring(3, 6);
		String p3 = numberKey.substring(6);
		return p1 + "/" + p2 + "/" + p3;
	}
	
	private String numberKeyToPath(String numberKey) {
		return getKeyPath(numberKeyToNumberKeyRelativePath(numberKey));
	}

	private String intToCounter(long i) throws Exception {
		if (i < 0)
			throw new Exception("Counters must be non-negative");
		if (i > 9999999999L)
			throw new
				Exception("Counters are only allowed with up to 10 digits");
		
		// Convert long to String number, with left zero padding to 10 digits
		return String.format("%1$010d", i);
	}
	
	// TODO - not sure if this should be part of interface
	public String getHighestCounter() throws Exception {
		// Fetch all counters.  There will likely be 1 counter, but may
		// be multiple (or zero).
		
		ZooKeeper zkc = getPlugin().getClient();
		String numberKeyPath = getNumberKeyPath();
		List<String> list = zkc.getChildren(numberKeyPath, false);

		// Iterate through list and find the greatest counter
		
		String high = null;
		Iterator<String> iter = list.iterator();
		while (iter.hasNext()) {
			String current = iter.next();
			if (high == null || current.compareTo(high) > 0)
				high = current;
		}
		
		return high;
	}
	
	public void generateUniqueNumericKey(MetastoreCallback<String> cb) {		
		ZooKeeperMetastorePlugin plugin = getPlugin();
		ZooKeeper zkc = plugin.getClient();
		String numberKeyPath = getNumberKeyPath();

		int rc = 0;
		String errorDescription = null;		
		String autogenKeyPath;
		String autogenCounter;
		
		// TODO - replace with async
		try {
			// Generate a sequential node in "counter/" to get a unique id to
			// use as the record key.
			
			// sequential requires trailing "/" to make non-prefixed counter.
			autogenKeyPath = zkc.create((numberKeyPath + "/"), new byte [0],
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			autogenCounter = autogenKeyPath.substring(numberKeyPath.length() + 1);			
			

			// Delete the persistent node in "<path_to_table>/counter" which was
			// created by the previous call (by anyone) to addRecord().  Thus
			// the counter node will have at most one child counter which is the
			// greatest one (for a brief period it will have two).  A fatal
			// error could leave an extra counter, and thus we should account
			// for this (e.g. getHighestCounter should not assume a single
			// counter).
			long i = Long.parseLong(autogenCounter);
			if (i > 0) {
				String prevCounter = intToCounter(i - 1);
				String prevActualPath = numberKeyPath + "/" + prevCounter;
				zkc.delete(prevActualPath, -1);
			}
		} catch (Exception e) {
			rc = 1;
			errorDescription = e.toString();
			autogenCounter = null;
		}
		cb.complete(rc, errorDescription, autogenCounter);
	}	
	
	/**
	 * Inserts record at the "end" of the table with an autogenerated
	 * incrementing id.  Call is "restartable" (may dynamically fail and be
	 * called with same arguments to succeed).  Any partially created state
	 * (partial path) is ignored by scan.
	 * @param cb - callback with autogenerated key that got added.
	 */
	public void addRecord(MetastoreCallback<String> cb, String value) {	
		int rc = 0;
		String errorDescription = null;		
		String autogenCounter;

		// TODO - replace with async
		try {
			// Get unique key
			final MetastoreCompletion<String> completion =
					new MetastoreCompletion<String> ();
			generateUniqueNumericKey (completion);		
			autogenCounter = completion.await ();
			
			final MetastoreCompletion<String> completion2 =
					new MetastoreCompletion<String> ();
			put(completion2, autogenCounter, value);
			autogenCounter = completion2.await();
		} catch (Exception e) {
			rc = 1;
			errorDescription = e.toString();
			autogenCounter = null;
		}
		cb.complete(rc, errorDescription, autogenCounter);
	}
	
	public void scan(MetastoreCallback<ScanResult> cb, String firstKey,
		String lastKey) {
		
		try {
			ScanResult result = doScan(firstKey, lastKey);
			int rc = 0;
			cb.complete(rc, null, result);
		} catch (Exception e) {
			cb.complete(1, "Error: " + e.toString(), null);
		}
	}
	
	public void get(MetastoreCallback<String> cb, String key) {
		super.get (cb, numberKeyToNumberKeyRelativePath (key));
	}
	
	public void put(MetastoreCallback<String> cb, String key, String value) {
		// TODO - maybe we should check key and throw exception?
	
		String counter = key;
		ZooKeeperMetastorePlugin plugin = getPlugin();
		ZooKeeperUtil zku = plugin.getUtil();

		int rc = 0;
		String errorDescription = null;
		
		byte [] valueBytes = ZooKeeperMetastorePluginUtil.encode(value);

		// TODO - replace with async
		try {
			// Create intermediate levels if necessary, as well as last
			// node (record itself)
			// Create a new record in "records/" with the unique key.  The
			// unique key is transformed s.t. it has 3 tree levels. e.g.
			// "0010020003" => "001/002/0003".  This splits the keys into
			// "partitions" which are of a size that is smaller than the max
			// fetch size for ZK client (i.e. a single getChildren() call).
			// If we are adding the first node in a partition, we must add the
			// new intermediate path nodes.  ensurePathExists() will potentially
			// create several levels in the actualPath.
			
			String actualPath = numberKeyToPath(counter);
			String assumedExistsPath = getRecordPath();
			zku.ensurePathExists(actualPath, assumedExistsPath, valueBytes);
		} catch (Exception e) {
			rc = 1;
			errorDescription = e.toString();
			counter = null;
		}
		cb.complete(rc, errorDescription, counter);
	}
	
	// TODO - needs to also potentially remove hierarchical levels
	// analogous to how addRecord does
	public void remove(MetastoreCallback<String> cb, String key) {
		super.remove(cb, numberKeyToNumberKeyRelativePath (key));
	}
	
	public void compareAndPut(MetastoreCallback<Boolean> cb, String key,
		String oldValue, String newValue) {
		super.compareAndPut(cb, numberKeyToNumberKeyRelativePath (key),
			oldValue, newValue);
	}

	
	// Key = NumberKey = 0010001234
	// NumberKeyRelativePath = 001/000/1234
	// KeyPath = /bookkeeper/metastore/zookeeper/tables/tn/records/001/000/1234

	
	private ScanResult doScan(String firstKey, String lastKey)
		throws Exception {
		
		ZooKeeperUtil zku = getPlugin().getUtil();
		
		// Convert keys to rel key paths, e.g. 0010020003 => 001/002/0003 to
		// pass into iterator
		String firstNumberKeyPath = firstKey == null ? null :
			numberKeyToNumberKeyRelativePath(firstKey);
		String lastNumberKeyPath = lastKey == null ? null :
			numberKeyToNumberKeyRelativePath(lastKey);
		ZooKeeperIterator iter =
			new ZooKeeperIterator(zku, getRecordPath(), firstNumberKeyPath,
					lastNumberKeyPath, 3);
		
		Vector<String> vector = new Vector<String> ();
		while (iter.hasNext()) {
			ZooKeeperIterator.PathResult result = iter.next();
			String relativeParentPath = result.getParentPath();
			String numericPrefix = relativeParentPath.replaceAll("/", "");
			Vector<String> children = result.getChildList();
			
			// TODO make sure we will get an empty array always and never null.
			int sz = children.size();
			for (int i=0 ; i < sz ; i++)
			{
				// Reconstruct the key from the numeric prefix (first 6 digits)
				// and child numeric part (last 4 digits)
				vector.add(numericPrefix + children.elementAt(i));
			}
		}

		
		// SimpleScanResult holds all of the MetastoreTableItems in memory.
		// Since these will be ZooKeeperMetastoreTableItems, they defer the
		// actual fetch of the data (using the keyPath) until scan.next().
		// This is also why Metastore.next() throws an exception.
		return new ZooKeeperScanResult(this, vector.iterator());
	}

}
