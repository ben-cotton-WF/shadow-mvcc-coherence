package com.sixwhits.cohmvcc.domain;

import java.util.NavigableSet;

import com.sixwhits.cohmvcc.pof.SortedSetCodec;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

@Portable
public class TransactionSetWrapper {
	@PortableProperty(value = 0, codec = SortedSetCodec.class)
	private NavigableSet<TransactionId> transactionIdSet;

	public NavigableSet<TransactionId> getTransactionIdSet() {
		return transactionIdSet;
	}

	public void setTransactionIdSet(NavigableSet<TransactionId> transactionIdSet) {
		this.transactionIdSet = transactionIdSet;
	}

}
