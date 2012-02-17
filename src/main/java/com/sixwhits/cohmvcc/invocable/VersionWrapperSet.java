package com.sixwhits.cohmvcc.invocable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.tangosol.util.BinaryEntry;

public class VersionWrapperSet implements Set<BinaryEntry> {
	private final Set<BinaryEntry> versionedSet;

	public VersionWrapperSet(Set<BinaryEntry> versionedSet) {
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

	public Iterator<BinaryEntry> iterator() {
		return new Iterator<BinaryEntry>() {
			
			private Iterator<BinaryEntry> underlying = versionedSet.iterator();

			@Override
			public boolean hasNext() {
				return underlying.hasNext();
			}

			@Override
			public BinaryEntry next() {
				return new VersionCacheEntryWrapper(underlying.next());
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

	public boolean add(BinaryEntry e) {
		throw new UnsupportedOperationException("set is read only");
	}

	public boolean remove(Object o) {
		throw new UnsupportedOperationException("set is read only");
	}

	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException("thought this wouldn't be called");
	}

	public boolean addAll(Collection<? extends BinaryEntry> c) {
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
