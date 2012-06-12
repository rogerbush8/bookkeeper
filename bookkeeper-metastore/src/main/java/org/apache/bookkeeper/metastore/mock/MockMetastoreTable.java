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
package org.apache.bookkeeper.metastore.mock;


import java.util.TreeMap;

import org.apache.bookkeeper.metastore.*;


public class MockMetastoreTable implements MetastoreTable {
	
	private String name;
	TreeMap<String, String> map;
	MockMetastorePlugin plugin;

	public MockMetastoreTable (MockMetastorePlugin plugin, String name) {
		this.plugin = plugin;
		this.map = new TreeMap<String,String> ();
		this.name = name;
	}
	
	public String getName () {
		return this.name;
	}
	
	public void get (MetastoreCallback<String> cb, String key) {
		String v = mockGet (key);
		int rc = 0;
		cb.complete (rc, null, v);		
	}
	
	public void put (MetastoreCallback<String> cb, String key, String value) {
		mockPut (key, value);
		int rc = 0;
		cb.complete (rc, null, value);
	}
	
	public void remove (MetastoreCallback<String> cb, String key) {
		mockRemove (key);
		int rc = 0;
		cb.complete (rc, null, key);
	}
	
	public void compareAndPut (MetastoreCallback<Boolean> cb, String key, String oldValue, String newValue) {
		boolean success = mockCompareAndPut (key, oldValue, newValue);
		int rc = 0;
		cb.complete (rc, null, new Boolean (success));
	}
	

	MockMetastorePlugin getPlugin() {
		return plugin;
	}

	private String mockGet (String key) {
		return map.get (key);
	}

	
	private void mockPut (String key, String value) {	
		map.put (key, value);
	}
	
	private void mockRemove (String key) {
		map.remove (key);
	}
		
	private boolean mockCompareAndPut (String key, String oldValue, String newValue) {
		String v = map.get (key);
		
		if ( ! v.equals(oldValue))
			return false;
		
		map.put (key, newValue);
		
		return true;
	}
}
