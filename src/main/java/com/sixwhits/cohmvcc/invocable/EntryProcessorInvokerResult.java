package com.sixwhits.cohmvcc.invocable;

import java.util.Map;

import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.partition.PartitionSet;

@Portable
public class EntryProcessorInvokerResult<K, R> {
	
	public static final int POF_PARTITIONS = 0;
	@PortableProperty(POF_PARTITIONS)
	private PartitionSet partitions;
	public static final int POF_RESULTS = 1;
	@PortableProperty(POF_RESULTS)
	private Map<K, R> resultMap;
	public static final int POF_RETRY = 2;
	@PortableProperty(POF_RETRY)
	private Map<K, VersionedKey<K>> retryMap;
	
	public EntryProcessorInvokerResult() {
		super();
	}
	
	public EntryProcessorInvokerResult(PartitionSet partitions,
			Map<K, R> resultMap, Map<K, VersionedKey<K>> retryMap) {
		super();
		this.partitions = partitions;
		this.resultMap = resultMap;
		this.retryMap = retryMap;
	}
	
	public PartitionSet getPartitions() {
		return partitions;
	}
	
	public Map<K, R> getResultMap() {
		return resultMap;
	}

	public Map<K, VersionedKey<K>> getRetryMap() {
		return retryMap;
	}

}
