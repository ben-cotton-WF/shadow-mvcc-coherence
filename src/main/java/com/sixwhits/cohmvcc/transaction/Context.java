package com.sixwhits.cohmvcc.transaction;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.net.NamedCache;

public interface Context {
	
	public TransactionId getCurrentTransaction();
	public void commit();
	public void rollback();
	public NamedCache getCache(String cacheName);

}
