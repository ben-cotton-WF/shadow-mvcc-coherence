package com.sixwhits.cohmvcc.cache.internal;

import java.util.concurrent.Semaphore;

import com.tangosol.util.AbstractMapListener;
import com.tangosol.util.MapEvent;

public class VersionCommitListener extends AbstractMapListener {

	private final Semaphore completeFlag;
	
	public VersionCommitListener() {
		super();
		this.completeFlag = new Semaphore(0);
	}

	@Override
	public void entryUpdated(MapEvent mapevent) {
		completeFlag.release();
	}
	
	@Override
	public void entryDeleted(MapEvent mapevent) {
		completeFlag.release();
	}
	
	public void waitForCommit() {
		try {
			completeFlag.acquire();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

}
