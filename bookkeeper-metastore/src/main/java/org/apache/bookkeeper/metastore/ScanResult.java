package org.apache.bookkeeper.metastore;

public interface ScanResult {
	public boolean hasNext();
	public MetastoreTableItem next() throws Exception;
}
