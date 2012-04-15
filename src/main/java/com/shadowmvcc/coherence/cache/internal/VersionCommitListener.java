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

package com.shadowmvcc.coherence.cache.internal;

import java.util.concurrent.Semaphore;

import com.tangosol.util.AbstractMapListener;
import com.tangosol.util.MapEvent;

/**
 * {@code MapListener} implementation used to wait for a cache entry to be committed.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class VersionCommitListener extends AbstractMapListener {

    private final Semaphore completeFlag;

    /**
     * Constructor.
     */
    public VersionCommitListener() {
        super();
        this.completeFlag = new Semaphore(0);
    }

    @Override
    public void entryUpdated(final MapEvent mapevent) {
        completeFlag.release();
    }

    @Override
    public void entryDeleted(final MapEvent mapevent) {
        completeFlag.release();
    }

    /**
     * A call to this method will block until the entry being monitored has been
     * committed or rolled back.
     */
    public void waitForCommit() {
        try {
            completeFlag.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
