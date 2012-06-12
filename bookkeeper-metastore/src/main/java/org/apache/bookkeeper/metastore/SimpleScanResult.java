
package org.apache.bookkeeper.metastore;

import java.util.Iterator;
import java.util.Vector;


public class SimpleScanResult implements ScanResult {

	private MetastorePlugin plugin;
	private Vector<MetastoreTableItem> vector = new Vector<MetastoreTableItem> ();
	private Iterator<MetastoreTableItem> iterator;
	
	public SimpleScanResult(MetastorePlugin plugin, Iterator<MetastoreTableItem> iterator) {
		this.plugin = plugin;
		
		while (iterator.hasNext())
			vector.add(iterator.next());
		this.iterator = vector.iterator();
	}
	
	public boolean hasNext() {
		return iterator.hasNext();
	}
	
	public MetastoreTableItem next() {
		return iterator.next();
	}
	
	public MetastorePlugin getPlugin() {
		return plugin;
	}
	
}
