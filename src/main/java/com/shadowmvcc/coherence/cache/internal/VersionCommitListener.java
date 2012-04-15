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
