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

package com.shadowmvcc.coherence.domain;

import com.shadowmvcc.coherence.cache.CacheName;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Encapsulate the result of a single {@code EntryProcessor} invocation. This
 * may be the actual return value of a wrapped {@code EntryProcessor}, or the
 * version cache key of an uncommitted entry that prevented completion.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 * @param <K> the cache logical key type
 * @param <R> the wrapped {@code EntryProcessor} result type
 */
@Portable
public class ProcessorResult<K, R> {

    @PortableProperty(0) private R result;
    @PortableProperty(1) private VersionCacheKey<K> waitKey;
    @PortableProperty(2) private boolean changed;
    @PortableProperty(3) private boolean returnResult;

    /**
     *  Default constructor for POF use only.
     */
    public ProcessorResult() {
        super();
    }

    /**
     * Constructor for a result indicating a wait is required for an uncommitted entry.
     * The cache containing the uncommitted entry may not be the same as the one on which
     * the original operation was performed in the case of an EntryProcessor that
     * updates backing maps of other MVCC caches
     * @param waitCacheName name of the cache containing the uncommitted entry
     * @param waitKey the version cache key of an uncommitted entry
     */
    public ProcessorResult(final CacheName waitCacheName, final VersionedKey<K> waitKey) {
        super();
        this.result = null;
        this.waitKey = new VersionCacheKey<K>(waitCacheName, waitKey);
        this.changed = false;
        this.returnResult = false;
    }
    /**
     * Constructor for a successful invocation.
     * @param result the {@code EntryProcessor} result
     * @param changed true if the processor invocation created a new version
     * @param returnResult false if this entry was not included in the result map from {@code processAll}
     */
    public ProcessorResult(final R result,
            final boolean changed, final boolean returnResult) {
        super();
        this.result = result;
        this.waitKey = null;
        this.changed = changed;
        this.returnResult = returnResult;
    }

    /**
     * @return the {@code EntryProcessor} result or null
     * if processing could not proceed because of an uncommitted entry
     */
    public R getResult() {
        return result;
    }

    /**
     * Does this result represent a wait for an uncommitted entry?
     * @return true if this result represents an uncommitted entry
     */
    public boolean isUncommitted() {
        return waitKey != null;
    }

    /**
     * Return the uncommitted entry key that we must wait for.
     * @return the version cache key that prevented execution or null
     */
    public VersionCacheKey<K> getWaitKey() {
        return waitKey;
    }

    /**
     * Did invocation create a new version.
     * @return true if a new version was created
     */
    public boolean isChanged() {
        return changed;
    }

    /**
     * Should the result be returned to the caller?
     * @return true if the result should be returned to the caller
     */
    public boolean isReturnResult() {
        return returnResult;
    }

}
