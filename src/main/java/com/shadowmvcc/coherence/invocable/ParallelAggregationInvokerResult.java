/*

Copyright 2012 Shadowmist Ltd.

This file is part of Shadow MVCC for Oracle Coherence.

Shadow MVCC for Oracle Coherence is free software: you can redistribute 
it and/or modify it under the terms of the GNU General Public License 
as published by the Free Software Foundation, either version 3 of the 
License, or (at your option) any later version.

Shadow MVCC for Oracle Coherence is distributed in the hope that it 
will be useful, but WITHOUT ANY WARRANTY; without even the implied 
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See 
the GNU General Public License for more details.
                        
You should have received a copy of the GNU General Public License
along with Shadow MVCC for Oracle Coherence.  If not, see 
<http://www.gnu.org/licenses/>.

*/

package com.shadowmvcc.coherence.invocable;

import java.util.Map;

import com.shadowmvcc.coherence.domain.VersionCacheKey;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Result type from a {@code ParallelAggregationInvoker}. Encapsulates a 
 * partial aggregation result, the set of partitions processed, and
 * a map of entries not aggregated because of uncommitted versions
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> the logical cache key
 * @param <R> the aggregator result type
 */
@Portable
public class ParallelAggregationInvokerResult<K, R> {

    public static final int POF_RESULT = 0;
    @PortableProperty(POF_RESULT) private R result;
    public static final int POF_RETRY = 1;
    @PortableProperty(POF_RETRY) private Map<K, VersionCacheKey<K>> retryMap;

    /**
     *  Default constructor for POF use only.
     */
    public ParallelAggregationInvokerResult() {
        super();
    }

    /**
     * @param result the partial aggregation result
     * @param retryMap the uncommitted entries
     */
    public ParallelAggregationInvokerResult(final R result, final Map<K, VersionCacheKey<K>> retryMap) {
        super();
        this.result = result;
        this.retryMap = retryMap;
    }

    /**
     * @return the partial invocation result
     */
    public R getResult() {
        return result;
    }

    /**
     * @return the map of uncommitted versions key is logical key, value is version cache key
     */
    public Map<K, VersionCacheKey<K>> getRetryMap() {
        return retryMap;
    }

}
