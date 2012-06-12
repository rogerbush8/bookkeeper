package org.apache.bookkeeper.metastore.testutil;

import org.apache.bookkeeper.metastore.*;

import java.util.concurrent.TimeoutException;

public class MetastoreScannableTableAsyncToSyncConverter extends MetastoreTableAsyncToSyncConverter {
	
	public MetastoreScannableTableAsyncToSyncConverter (MetastoreScannableTable table) {
		super (table);
	}
	
	public MetastoreScannableTable getScannableTable () {
		return (MetastoreScannableTable) getTable ();
	}
	
	public String addRecord (String value) throws InterruptedException, TimeoutException, Exception {
		final MetastoreCompletion<String> cb = new MetastoreCompletion<String> ();
		getScannableTable ().addRecord (cb, value);
		return cb.await ();
	}
	
	public ScanResult scan (String firstKey, String lastKey) throws InterruptedException, TimeoutException, Exception {	
		final MetastoreCompletion<ScanResult> cb = new MetastoreCompletion<ScanResult> ();
		getScannableTable ().scan (cb, firstKey, lastKey);
		return cb.await ();
	}
}
