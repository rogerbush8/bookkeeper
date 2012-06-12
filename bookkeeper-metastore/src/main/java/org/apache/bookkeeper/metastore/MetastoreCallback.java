package org.apache.bookkeeper.metastore;

public abstract class MetastoreCallback<T> {
	public abstract void complete (int rc, String errorDescription, T value);
}
