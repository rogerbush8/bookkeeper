package org.apache.bookkeeper.metastore.testutil;

import org.apache.bookkeeper.metastore.MetastoreCallback;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Convenience class to convert async call to sync by waiting on a callback result.
 * TODO - it may be possible to convert to 
 * @author rbush
 *
 * @param <T> - The result type.  Must be an Object.
 */
public class MetastoreCompletion<T> extends MetastoreCallback<T> {

	private static long defaultMillisecondTimeout = 3000;
	
	private T value = null;
	private int rc = 0;
	private String errorDescription = null;
	private final CountDownLatch latch = new CountDownLatch (1);
	
	public static void setDefaultMillisecondTimeout (long millis) {
		defaultMillisecondTimeout = millis;
	}
	
	public static long getDefaultMillisecondTimeout () {
		return defaultMillisecondTimeout;
	}
	
	public T await () throws TimeoutException, InterruptedException, Exception
	{
		return await (defaultMillisecondTimeout);
	}
	
	public T await (long timeoutInMilliseconds) throws TimeoutException, InterruptedException, Exception
	{
		if ( ! latch.await (timeoutInMilliseconds, TimeUnit.MILLISECONDS))
			throw new TimeoutException
				("Timeout await(ing) on value, timeout=" + timeoutInMilliseconds + "milliseconds");
		
		if (this.rc == 0)
			return value;
		else
			throw new Exception ("Exception rc=" + this.rc +
					", description=" + this.errorDescription);
	}
	
	public void complete (int rc, String errorDescription, T value) {		
		this.rc = rc;
		if (rc == 0)
			this.errorDescription = null;
		else
			this.errorDescription = errorDescription == null ? "" : errorDescription;
		
		this.value = value;
		latch.countDown ();
	}
}
