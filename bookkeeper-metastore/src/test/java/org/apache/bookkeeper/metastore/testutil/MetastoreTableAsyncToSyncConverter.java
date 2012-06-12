package org.apache.bookkeeper.metastore.testutil;
/**
 * Copyright 2012 Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

//import java.util.concurrent.*;

import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.metastore.*;


// Converts async calls to sync calls for MetadataTable.  Currently not
// intended to be used other than for simple functional tests, however,
// could be developed into a sync API.

public class MetastoreTableAsyncToSyncConverter {
	private MetastoreTable table;
	
	public MetastoreTableAsyncToSyncConverter (MetastoreTable table) {
		this.table = table;
	}
	
	public MetastoreTable getTable () {
		return this.table;
	}
	
	public String get (String key) throws InterruptedException, TimeoutException, Exception {
		final MetastoreCompletion<String> completion = new MetastoreCompletion<String> ();
		this.table.get (completion, key);		
		return completion.await ();
	}
	
	public void put (String key, String value) throws InterruptedException, TimeoutException, Exception {
		final MetastoreCompletion<String> completion = new MetastoreCompletion<String> ();
		this.table.put (completion, key, value);		
		completion.await ();
		return;
	}
	
	public void remove (String key) throws InterruptedException, TimeoutException, Exception {
		final MetastoreCompletion<String> completion = new MetastoreCompletion<String> ();
		this.table.remove (completion, key);		
		completion.await ();
		return;
	}
	
	public boolean compareAndPut (String key, String oldValue, String newValue) throws InterruptedException, TimeoutException, Exception {
		final MetastoreCompletion<Boolean> completion = new MetastoreCompletion<Boolean> ();
		this.table.compareAndPut (completion, key, oldValue, newValue);		
		return completion.await ().booleanValue ();
	}
}
