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

import org.junit.Before;

import com.shadowmvcc.coherence.domain.SampleDomainObject;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionedKey;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.shadowmvcc.coherence.transaction.internal.EntryCommitProcessor;
import com.shadowmvcc.coherence.transaction.internal.EntryRollbackProcessor;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

/**
 * Abstract base class for testing the MVCCTransactionalCache, split into
 * several test classes because it's too big otherwise.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class AbstractMVCCTransactionalCacheTest extends AbstractLittlegridTest {

    private static final String TESTCACHEMAME = "testcache";
    protected MVCCTransactionalCacheImpl<Integer, SampleDomainObject> cache;
    protected final TransactionId ts1 = new TransactionId(BASETIME, 0, 0);
    protected final TransactionId ts2 = new TransactionId(BASETIME + 1, 0, 0);
    protected final TransactionId ts3 = new TransactionId(BASETIME + 2, 0, 0);
    protected final TransactionId ts4 = new TransactionId(BASETIME + 3, 0, 0);
    protected final TransactionId ts5 = new TransactionId(BASETIME + 4, 0, 0);
    protected final TransactionId ts6 = new TransactionId(BASETIME + 5, 0, 0);
    protected final TransactionId ts7 = new TransactionId(BASETIME + 6, 0, 0);

    /**
     * create cluster and initialise cache.
     */
    @Before
    public void setUp() {
    
        System.out.println("******initialise cache");
        cache = new MVCCTransactionalCacheImpl<Integer, SampleDomainObject>(TESTCACHEMAME, "InvocationService");
    }

    /**
     * Utility method to spawn a thread that later commits a value.
     * @param ts timestamp 
     * @param key key
     */
    protected void asynchCommit(final TransactionId ts, final Integer key) {
        new Thread(new Runnable() {
    
            @Override
            public void run() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
                NamedCache vcache = CacheFactory.getCache(cache.getMVCCCacheName().getVersionCacheName());
                vcache.invoke(new VersionedKey<Integer>(key, ts), new EntryCommitProcessor());
            }
        }).start();
    }

    /**
     * Utility method to spawn a thread that later commits a value.
     * @param flag semaphore to release after the commit
     * @param ts timestamp 
     * @param key key
     */
    protected void asynchCommit(final Semaphore flag, final TransactionId ts, final Integer key) {
        new Thread(new Runnable() {
    
            @Override
            public void run() {
                try {
                    flag.acquire();
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
                NamedCache vcache = CacheFactory.getCache(cache.getMVCCCacheName().getVersionCacheName());
                vcache.invoke(new VersionedKey<Integer>(key, ts), new EntryCommitProcessor());
            }
        }).start();
    }

    /**
     * Utility method to spawn a thread that later rolls back a value.
     * @param ts timestamp 
     * @param key key
     */
    protected void asynchRollback(final TransactionId ts, final Integer key) {
        Thread rbThread = new Thread(new Runnable() {
    
            @Override
            public void run() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
                NamedCache vcache = CacheFactory.getCache(cache.getMVCCCacheName().getVersionCacheName());
                vcache.invoke(new VersionedKey<Integer>(key, ts), new EntryRollbackProcessor());
            }
        });
    
        rbThread.start();
    }

}
