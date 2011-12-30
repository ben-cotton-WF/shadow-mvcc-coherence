package com.sixwhits.cohmvcc.domain;

import java.util.Collection;
import java.util.Collections;
import java.util.NavigableSet;
import java.util.TreeSet;

public class KeyTimestamps {
	
	private NavigableSet<Long> readTimestamps;
	private NavigableSet<Long> writeTimestamps;
	
	public KeyTimestamps() {
		super();
		this.readTimestamps = new TreeSet<Long>();
		this.writeTimestamps = new TreeSet<Long>();
	}

	public KeyTimestamps(Collection<Long> readTimestamps,
			Collection<Long> writeTimestamps) {
		super();
		this.readTimestamps = new TreeSet<Long>(readTimestamps);
		this.writeTimestamps = new TreeSet<Long>(writeTimestamps);
	}

	public Collection<Long> getReadTimestamps() {
		return Collections.unmodifiableCollection(readTimestamps);
	}

	public void setReadTimestamps(Collection<Long> readTimestamps) {
		this.readTimestamps = new TreeSet<Long>(readTimestamps);
	}

	public Collection<Long> getWriteTimestamps() {
		return Collections.unmodifiableCollection(writeTimestamps);
	}

	public void setWriteTimestamps(Collection<Long> writeTimestamps) {
		this.writeTimestamps = new TreeSet<Long>(writeTimestamps);
	}
	
	public void addReadTimestamp(Long timestamp) {
		readTimestamps.add(timestamp);
	}

	public void addWriteTimestamp(Long timestamp) {
		writeTimestamps.add(timestamp);
	}
	
	public Long getPrecedingReadTimestamp(Long timestamp) {
		return readTimestamps.floor(timestamp);
	}

	public Long getPrecedingWriteTimestamp(Long timestamp) {
		return writeTimestamps.floor(timestamp);
	}
	
}
