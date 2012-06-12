package org.apache.bookkeeper.metastore.zookeeper;

import java.util.Collections;
import java.util.Vector;
import java.util.List;

/**
 * @author rbush
 * Iterates over child nodes of a certain depth from the standpoint of a
 * rootPath.  Imposes a sort order over all nodes (sorts as it pulls).  Each
 * iteration (next()) provides a list of (lexicographically sorted) child keys,
 * as well as the parent path for the list.  This corresponds to a call to
 * zk.getChildren() (potentially one call for each level up to the depth).  If
 * a path is less than the specified depth, a null pointer will be returned
 * instead of a list.  Thus this lets us iterate at a particular depth across
 * the children, which is exactly what we want for our zookeeper hierarchical
 * scan table (ZooKeeperMetastoreScannableTable).  Optimal iteration requires
 * some knowledge of the boundaries for groups of nodes (so that the minimum
 * number of zk.getChildren() calls are made).
 */
public class ZooKeeperIterator {
	ZooKeeperUtil zku;
	private String rootPath;
	private int depth;
	private Vector<Vector<String>> queues;
	private Vector<String> startRelativeElements;
	private Vector<String> endRelativeElements;
	private int startBoundaryDepth;
	private int endBoundaryDepth;
	
	public class PathResult {
		private String parentPath;
		private int depth;
		private Vector<String> childList;
		PathResult(String parentPath, int depth) {
			this.parentPath = parentPath;
			this.depth = depth;
			this.childList = null;
		}
		
		public String getParentPath() {
			return parentPath;
		}
		
		public int getDepth() {
			return depth;
		}
		
		public Vector<String> getChildList() {
			return childList;
		}
		
		void setChildList(Vector<String> childList) {
			this.childList = childList;
		}
	}
	
	
	/**
	 * 
	 * @param zku
	 * @param rootPath
	 * @param startRelativePath
	 * @param endRelativePath
	 * @param depth
	 * 
	 * Zoo
	 */
	public ZooKeeperIterator(ZooKeeperUtil zku, String rootPath,
			String startRelativePath, String endRelativePath, int depth)
			throws Exception {
		
		this.depth = depth;
		this.zku = zku;
		this.rootPath = rootPath;
		
		// Parse individual elements to vectors for processing in pull()
		
		startRelativeElements = new Vector<String>();
		endRelativeElements = new Vector<String>();
		
		if (startRelativePath != null) {
			String[] arr1 = startRelativePath.split("/");
			for (int i=0 ; i < arr1.length ; i++)
				startRelativeElements.add(arr1[i]);
		}
		
		if (endRelativePath != null) {
			String[] arr2 = endRelativePath.split("/");
			for (int i=0 ; i < arr2.length ; i++)
				endRelativeElements.add(arr2[i]);
		}
		
		startBoundaryDepth = 0;
		endBoundaryDepth = 0;
		
		
		// Internal state of the iterator consists of a vector of string queues.
		// Each queue in "queues" will hold the result of a getChildren() call
		// (lexicographically sorted) at the appropriate depth.  If an entry in
		// "queues" is null, it means that the item hasn't been pulled yet, or
		// does not exist.

		
		// Initialize queues with the number of depth (tree depth) we will
		// iterate over.  Each depth is a queue which we will process FIFO.  If
		// queue is null, it needs to be pulled.
		
		queues = new Vector<Vector<String>>();
		for (int i=0 ; i < depth ; i++)
			queues.add(null);
		
		init();	
	}
	
	private void init() throws Exception {
		pull();
		
		// If the first depth has 0 elements, then it is an empty tree.  We
		// handle this end case by setting the first queue to null.
		
		Vector<String> q = queues.elementAt(0);
		if (q.size() == 0) {
			queues.set(0, null);
			return;
		}
	}
	
	
	/**
	 * Pulls the next required depth of children to "queues", and removes
	 * elements as necessary based on startRelativePath and endRelativePath.
	 * @param depth
	 * @return
	 * @throws Exception
	 */
	private void pull() throws Exception {
		String curPath = rootPath;
		for (int i=0 ; i < depth ; i++) {
			Vector<String> q = queues.elementAt(i);
			
			// If the queue is empty, pull and set.  A null queue means "must
			// be pulled", whereas an empty queue is the result of pulling an
			// empty depth.
			
			if (q == null) {
				List<String> children = 
					zku.getClient().getChildren(curPath, false);
				q = new Vector<String>();
				q.addAll(children);
				
				// Sort children so we always access in a predictable order.
				
				Collections.sort(q);
				queues.set(i, q);

				
				// Need to conditionally remove items from the left (start)
				// or the right (end) based on startRelativePath and
				// endRelativePath given to iterator.  This is done by
				// matching the boundary at each depth, and progressing to
				// the next depth in the case of an exact match (equals), or
				// stopping (deactivate and set to -1) if we are gt/lt
				// for start/end.
				
				String startEl = startRelativeElements.size() > i ?
						startRelativeElements.elementAt(i) : null;
				
				// Handle left (start)
						
				if (startBoundaryDepth == i && startEl != null) {
					
					// At current depth, remove q els until we are >= to current
					
					int icmp = 0;
					int sz = q.size();
					for (int j=0 ; j < sz ; j++) {
						String el2 = q.elementAt(0);
						icmp = startEl.compareTo(el2);
						
						// If lt then done, completely
						
						if (icmp < 0) {
							// done, deactivate
							startBoundaryDepth = -1;
							break;
						}
						
						// if eq then done, processing continues next depth
						
						if (icmp == 0) {
							startBoundaryDepth++;
							break;
						}
						
						// gt.  Remove element and continue search
						
						q.remove(0);
					}
				}
				
				String endEl = endRelativeElements.size() > i ?
						endRelativeElements.elementAt(i) : null;
				
				// Handle right (end)
						
				if (endBoundaryDepth == i && endEl != null) {
					
					// At current depth in q, remove els until >= to current
					
					int lastIdx = 0;
					int icmp = 0;
					int sz = q.size();
					for (int j=sz - 1 ; j >= 0 ; j--) {
						lastIdx = q.size() - 1;
						String el2 = q.elementAt(lastIdx);
						icmp = endEl.compareTo(el2);
						
						// Deactivate endBoundaryDepth as we have passed it
						
						if (icmp > 0) {
							endBoundaryDepth = -1;
							break;
						}
						
						if (icmp == 0) {
							endBoundaryDepth++;
							break;
						}
												
						// gt.  Remove element and continue search
						
						q.remove(lastIdx);
					}
					
					// If last row and equal, remove item since boundaries are
					// "open on RHS" (i.e. don't include the end element)
					
					if (i == depth - 1 && icmp == 0)
						q.remove(lastIdx);	
				}	
			}
					
			if (q.size() == 0)
				return;
			
			curPath += "/" + q.elementAt(0);
		}
	}
	
	/**
	 * Uses the queues' state, which holds current tree nodes, to get the first
	 * nodes in each queue (current path).  The current path does not include
	 * the last depth (it is the parent path).
	 * @return
	 */
	private PathResult getCurrentRelativeParentPath() {
		String curPath = "";
		for (int i=0 ; i < depth ; i++) {
			Vector<String> q = queues.elementAt(i);
			
			// Do not call this before calling pull
			
			if (q == null || q.size() == 0)
				return new PathResult (curPath, i);
			
			// Don't add last path element
			
			if (i < depth - 1) {
				
				// Append next element to path
				
				if (curPath.length() == 0)
					curPath = q.elementAt(0);
				else
					curPath += "/" + q.elementAt(0);
			}
		}
		
		return new PathResult (curPath, depth - 1);
	}
	
	
	public boolean hasNext() throws Exception {
		// Since we use a "null depth" to indicate we are done, when the
		// top depth becomes null, it means there are no more.  If we ever have
		// state in the queues, then we have at least one more "element"
		return queues.elementAt(0) != null;
	}
	

	/**
	 * Removes elements in the queues which corresponds to "moving forward
	 * by one".
	 */
	private void moveForward() {
		
		// Remove leaf queue
		
		int idx = depth - 1;
		queues.set(idx,  null);
		
		// Remove one element from each parent required (if we remove a single
		// element causing an empty array, then we continue up).
		
		for (int i=idx - 1 ; i >= 0 ; i--) {
			Vector<String> q = queues.elementAt(i);
			if (q == null)
				continue;
			
			if (q.size() > 0)
				q.remove(0);
			
			if (q.size() == 0)
				queues.set(i,  null);
			else
				break;		
		}
	}
	


	/**
	 * @return
	 * @throws Exception
	 */
	public PathResult next() throws Exception {
		
		// Get parent path and current list
		
		PathResult rv = getCurrentRelativeParentPath();
		int idx = rv.getDepth();
		
		// If the last depth has a queue, return it.  If there is no last depth,
 		// then return a null queue.

		if (idx == depth - 1)
			rv.setChildList(queues.elementAt(idx));
		
		moveForward();
		if (queues.elementAt(0) != null)
			pull();
		
		return rv;
	}
}
