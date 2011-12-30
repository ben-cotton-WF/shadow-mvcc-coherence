package com.sixwhits.cohmvcc.transaction;

import com.tangosol.net.NamedCache;

public interface Context {
	
	public Long getCurrentTransaction();
	public void commit();
	public void rollback();
	public NamedCache getCache(String cacheName);

}
