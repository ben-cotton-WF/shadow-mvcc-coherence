package com.sixwhits.cohmvcc.invocable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.tangosol.util.BinaryEntry;
import com.tangosol.util.InvocableMap.Entry;

public class VersionWrapperSet implements Set<Entry> {
	private final Set<Entry> versionedSet;

	public VersionWrapperSet(Set<Entry> versionedSet) {
		super();
		this.versionedSet = versionedSet;
	}

	public int size() {
		return versionedSet.size();
	}

	public boolean isEmpty() {
		return versionedSet.isEmpty();
	}

	public boolean contains(Object o) {
		throw new UnsupportedOperationException("thought this wouldn't be called");
	}

	public Iterator<Entry> iterator() {
		return new Iterator<Entry>() {
			
			private Iterator<Entry> underlying = versionedSet.iterator();

			@Override
			public boolean hasNext() {
				return underlying.hasNext();
			}

			@SuppressWarnings({ "rawtypes" })
			@Override
			public Entry next() {
				Entry underlyingEntry = underlying.next();
				if (underlyingEntry instanceof BinaryEntry) {
					return new VersionCacheBinaryEntryWrapper((BinaryEntry)underlyingEntry);
				} else {
					return new VersionCacheEntryWrapper(underlyingEntry);
				}
			}

			@Override
			public void remove() {
				underlying.remove();
			}
		};
	}

	public Object[] toArray() {
		throw new UnsupportedOperationException("thought this wouldn't be called");
	}

	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException("thought this wouldn't be called");
	}

	public boolean add(Entry e) {
		throw new UnsupportedOperationException("set is read only");
	}

	public boolean remove(Object o) {
		throw new UnsupportedOperationException("set is read only");
	}

	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException("thought this wouldn't be called");
	}

	public boolean addAll(Collection<? extends Entry> c) {
		throw new UnsupportedOperationException("set is read only");
	}

	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("set is read only");
	}

	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("set is read only");
	}

	public void clear() {
		versionedSet.clear();
	}

	public boolean equals(Object o) {
		return versionedSet.equals(o);
	}

	public int hashCode() {
		return versionedSet.hashCode();
	}

}
