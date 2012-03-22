package com.sixwhits.cohmvcc.cache.internal;

import java.util.concurrent.Semaphore;

import org.junit.Before;

import com.sixwhits.cohmvcc.domain.SampleDomainObject;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.sixwhits.cohmvcc.testsupport.AbstractLittlegridTest;
import com.sixwhits.cohmvcc.transaction.internal.EntryCommitProcessor;
import com.sixwhits.cohmvcc.transaction.internal.EntryRollbackProcessor;
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
