package com.sixwhits.cohmvcc.domain;

import com.tangosol.net.cache.KeyAssociation;

public class VersionedKey implements KeyAssociation {
	
	private Object nativeKey;
	private long txTimeStamp;
	
	public VersionedKey(Object nativeKey, long txTimeStamp) {
		super();
		this.nativeKey = nativeKey;
		this.txTimeStamp = txTimeStamp;
	}

	public Object getNativeKey() {
		return nativeKey;
	}

	public void setNativeKey(Object nativeKey) {
		this.nativeKey = nativeKey;
	}

	public long getTxTimeStamp() {
		return txTimeStamp;
	}

	public void setTxTimeStamp(long txTimeStamp) {
		this.txTimeStamp = txTimeStamp;
	}

	public Object getAssociatedKey() {
		if (nativeKey instanceof KeyAssociation) {
			return ((KeyAssociation)nativeKey).getAssociatedKey();
		} else {
			return nativeKey.hashCode();
		}
	}
}
