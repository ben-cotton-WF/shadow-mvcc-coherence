package com.sixwhits.cohmvcc.invocable;

import java.util.Map;

import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.partition.PartitionSet;

@Portable
public class ParallelAggregationInvokerResult<K, R> {
	
	public static final int POF_PARTITIONS = 0;
	@PortableProperty(POF_PARTITIONS)
	private PartitionSet partitions;
	public static final int POF_RESULT = 1;
	@PortableProperty(POF_RESULT)
	private R result;
	public static final int POF_RETRY = 2;
	@PortableProperty(POF_RETRY)
	private Map<K, VersionedKey<K>> retryMap;
	
	public ParallelAggregationInvokerResult() {
		super();
	}
	
	public ParallelAggregationInvokerResult(PartitionSet partitions,
			R result, Map<K, VersionedKey<K>> retryMap) {
		super();
		this.partitions = partitions;
		this.result = result;
		this.retryMap = retryMap;
	}
	
	public PartitionSet getPartitions() {
		return partitions;
	}
	
	public R getResult() {
		return result;
	}

	public Map<K, VersionedKey<K>> getRetryMap() {
		return retryMap;
	}

}
