package org.apache.bookkeeper.metastore.zookeeper;

import org.apache.bookkeeper.metastore.*;
import org.apache.bookkeeper.metastore.testutil.MetastoreCompletion;

import java.util.Iterator;
import java.util.Vector;


public class ZooKeeperScanResult implements ScanResult {

	private ZooKeeperMetastoreScannableTable table;
	private Vector<String> vector = new Vector<String> ();
	private Iterator<String> iterator;
	
	public ZooKeeperScanResult(ZooKeeperMetastoreScannableTable table, Iterator<String> iterator) {
		this.table = table;
		
		while (iterator.hasNext())
			vector.add(iterator.next());
		
		this.iterator = vector.iterator();
	}
	
	public boolean hasNext() {
		return iterator.hasNext();
	}
	
	public MetastoreTableItem next() throws Exception {
		String key = iterator.next();
		final MetastoreCompletion<String> completion = new MetastoreCompletion<String> ();
		this.table.get (completion, key);		
		String value = completion.await ();
		return new MetastoreTableItem(key, value);
	}

}
