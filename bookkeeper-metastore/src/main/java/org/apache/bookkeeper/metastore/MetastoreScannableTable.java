package org.apache.bookkeeper.metastore;


public interface MetastoreScannableTable extends MetastoreTable {
	
	/**
	 * A unique, incrementing, numeric String key for the table.
	 * @param cb - callback returning String numeric key.
	 */
	public void generateUniqueNumericKey(MetastoreCallback<String> cb);
	
	/**
	 * Adds a record with an autogenerated, unique, numeric key.
	 * @param cb - callback returning String numeric key.
	 * @param value - value of record to save at generated key.
	 */
	public void addRecord (MetastoreCallback<String> cb, String value);

	/**
	 * Return a ScanResult which can be used to iterate over keys which fall
	 * between firstNumericKey (included) and lastNumericKey (excluded).
	 * @param cb - Callback returning ScanResult which contains keys.
	 * @param firstNumericKey - start range of keys (included).  null => first.
	 * @param lastNumericKey - end range of keys (excluded).  null => incl last.
	 */
	public void scan (MetastoreCallback<ScanResult> cb, String firstNumericKey,
		String lastNumericKey);
}
