package com.sixwhits.cohmvcc.domain;

import java.util.Collection;
import java.util.Collections;
import java.util.NavigableSet;
import java.util.TreeSet;

public class KeyTimestamps {
	
	private NavigableSet<VersionTimestamp> readTimestamps;
	
	public KeyTimestamps() {
		super();
		this.readTimestamps = new TreeSet<VersionTimestamp>();
	}

	public KeyTimestamps(Collection<VersionTimestamp> readTimestamps,
			Collection<VersionTimestamp> writeTimestamps) {
		super();
		this.readTimestamps = new TreeSet<VersionTimestamp>(readTimestamps);
	}

	public Collection<VersionTimestamp> getReadTimestamps() {
		return Collections.unmodifiableCollection(readTimestamps);
	}

	public void setReadTimestamps(Collection<VersionTimestamp> readTimestamps) {
		this.readTimestamps = new TreeSet<VersionTimestamp>(readTimestamps);
	}

	
}
