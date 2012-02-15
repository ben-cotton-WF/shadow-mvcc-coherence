package com.sixwhits.cohmvcc.invocable;

import com.tangosol.util.BinaryEntry;

public interface EntryWrapper extends BinaryEntry {

	public abstract boolean isRemove();

}