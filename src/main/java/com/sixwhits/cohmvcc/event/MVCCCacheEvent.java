package com.sixwhits.cohmvcc.event;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.net.cache.CacheEvent;
import com.tangosol.util.ObservableMap;

/**
 * 
 * Extends {@code CacheEvent} to provide additional transaction context.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class MVCCCacheEvent extends CacheEvent {
	
	public enum CommitStatus { open, commit, rollback }
	private static final long serialVersionUID = -1015113502920893252L;
	private final CommitStatus commitStatus;
	private final TransactionId transactionId;

	public MVCCCacheEvent(ObservableMap map, int nId, Object oKey,
			Object oValueOld, Object oValueNew, boolean synthetic, TransactionId transactionId, CommitStatus commitStatus) {
		super(map, nId, oKey, oValueOld, oValueNew, synthetic);
		this.commitStatus = commitStatus;
		this.transactionId = transactionId;
	}

	/**
	 * @return the transaction Id responsible for the change
	 */
	public TransactionId getTransactionId() {
		return transactionId;
	}

	/**
	 * @return the commit status of the event
	 */
	public CommitStatus getCommitStatus() {
		return commitStatus;
	}
	
	

}
