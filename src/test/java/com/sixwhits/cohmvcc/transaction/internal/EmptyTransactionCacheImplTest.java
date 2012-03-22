package com.sixwhits.cohmvcc.transaction.internal;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.sixwhits.cohmvcc.domain.TransactionCacheValue;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.TransactionProcStatus;
import com.sixwhits.cohmvcc.testsupport.AbstractLittlegridTest;
import com.sixwhits.cohmvcc.transaction.TransactionException;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

/**
 * Test the transaction cache implementation for basic
 * commit/rollback and exception states using empty transactions.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class EmptyTransactionCacheImplTest extends AbstractLittlegridTest {

    private NamedCache basetxcache;
    private TransactionCache transactionCache;
    private static final TransactionId TX = new TransactionId(BASETIME, 0, 0);
    
    /**
     * Set up the caches.
     */
    @Before
    public void setup() {
        basetxcache = CacheFactory.getCache(TransactionCacheImpl.CACHENAME);
        transactionCache = new TransactionCacheImpl(INVOCATIONSERVICENAME);
    }
    
    /**
     * Begin transaction, no baggage.
     */
    @Test
    public void testBeginTransaction() {
        
        transactionCache.beginTransaction(TX, readCommitted);
        
        assertEquals(TransactionProcStatus.open,
                ((TransactionCacheValue) basetxcache.get(TX)).getProcStatus());
        
    }
    
    /**
     * Duplicate transaction should throw a TransactionException.
     */
    @Test(expected = TransactionException.class)
    public void testBeginDuplicate() {

        transactionCache.beginTransaction(TX, readCommitted);

        transactionCache.beginTransaction(TX, readCommitted);

    }
    
    /**
     * Commit with no transaction should throw a TransactionException.
     */
    @SuppressWarnings("unchecked")
    @Test(expected = TransactionException.class)
    public void commitNonExistent() {
        
        transactionCache.commitTransaction(TX, Collections.EMPTY_MAP, Collections.EMPTY_MAP);
        
    }
    
    /**
     * Commit with no transaction should throw a TransactionException.
     */
    @SuppressWarnings("unchecked")
    @Test(expected = TransactionException.class)
    public void commitNotOpen() {
        
        basetxcache.put(TX, new TransactionCacheValue(TransactionProcStatus.rollingback, System.currentTimeMillis()));
        
        transactionCache.commitTransaction(TX, Collections.EMPTY_MAP, Collections.EMPTY_MAP);
        
    }
    
    /**
     * Test commit of an empty transaction.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void sunnyDayCommit() {

        transactionCache.beginTransaction(TX, readCommitted);

        transactionCache.commitTransaction(TX, Collections.EMPTY_MAP, Collections.EMPTY_MAP);
        
        assertNull(basetxcache.get(TX));

    }
    
    /**
     * Test rollback of an empty transaction.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void sunnyDayRollback() {

        transactionCache.beginTransaction(TX, readCommitted);

        transactionCache.rollbackTransaction(TX, Collections.EMPTY_MAP, Collections.EMPTY_MAP);
        
        assertNull(basetxcache.get(TX));

    }

}
