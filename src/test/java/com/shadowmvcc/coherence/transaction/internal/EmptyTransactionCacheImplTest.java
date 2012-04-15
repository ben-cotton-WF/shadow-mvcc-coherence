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

package com.shadowmvcc.coherence.transaction.internal;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readCommitted;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.domain.TransactionCacheValue;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.TransactionProcStatus;
import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.shadowmvcc.coherence.transaction.TransactionException;
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
