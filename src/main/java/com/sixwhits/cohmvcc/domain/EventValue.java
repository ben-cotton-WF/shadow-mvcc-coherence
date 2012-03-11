package com.sixwhits.cohmvcc.domain;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

@Portable
public class EventValue<V> {
	
	@PortableProperty(0)
	private boolean committed;
	@PortableProperty(1)
	private boolean deleted;
	@PortableProperty(2)
	private V value;
	
	public EventValue() {
		super();
	}

	public EventValue(boolean committed, boolean deleted, V value) {
		super();
		this.committed = committed;
		this.deleted = deleted;
		this.value = value;
	}

	public boolean isCommitted() {
		return committed;
	}

	public boolean isDeleted() {
		return deleted;
	}

	public V getValue() {
		return value;
	}
	
	
	

}
