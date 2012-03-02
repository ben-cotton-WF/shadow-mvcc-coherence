package com.sixwhits.cohmvcc.index;

import java.util.Map.Entry;

import com.sixwhits.cohmvcc.invocable.VersionCacheBinaryEntryWrapper;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.Filter;
import com.tangosol.util.filter.EntryFilter;

public class FilterWrapper implements EntryFilter {
	
	private final Filter delegate;

	public FilterWrapper(Filter delegate) {
		super();
		this.delegate = delegate;
	}

	@Override
	public boolean evaluate(Object obj) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public boolean evaluateEntry(@SuppressWarnings("rawtypes") Entry arg) {
		BinaryEntry entry = (BinaryEntry) arg;
		BinaryEntry wrappedEntry = new VersionCacheBinaryEntryWrapper(entry);
		if (delegate instanceof EntryFilter) {
			return ((EntryFilter)delegate).evaluateEntry(wrappedEntry);
		} else {
			return delegate.evaluate(wrappedEntry.getValue()); 
		}
	}

}
