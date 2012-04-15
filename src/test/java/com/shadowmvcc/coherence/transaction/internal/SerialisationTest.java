package com.shadowmvcc.coherence.transaction.internal;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.TransactionProcStatus;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;

/**
 * Serialisation test for transaction internal package.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class SerialisationTest {

    private ConfigurablePofContext pofContext;
    private static final TransactionId TRANSACTIONID =
            new TransactionId(40L * 365L * 24L * 60L * 60L * 1000L + 17, 124, 457);

    /**
     * Initialise POF context.
     */
    @Before
    public void setUp() {
        pofContext = new ConfigurablePofContext("mvcc-pof-config.xml");
    }

    /**
     * ReadMarkingProcessor.
     */
    @Test
    public void testReadMarkingProcessor() {
        ReadMarkingProcessor<Integer> obj = new ReadMarkingProcessor<Integer>(
                TRANSACTIONID, 
                IsolationLevel.serializable, new CacheName("acachename"));

        assertPofFidelity(obj);
    }

    /**
     * EntryCommitProcessor.
     */
    @Test
    public void testEntryCommitProcessor() {
        Object obj = new EntryCommitProcessor();
        assertPofFidelity(obj);
    }

    /**
     * EntryRollbackProcessor.
     */
    @Test
    public void testEntryRollbackProcessor() {
        Object obj = new EntryRollbackProcessor();
        assertPofFidelity(obj);
    }
    /**
     * ExistenceCheckProcessor.
     */
    @Test
    public void testExistenceCheckProcessor() {
        Object obj = new ExistenceCheckProcessor();
        assertPofFidelity(obj);
    }
    
    /**
     * FilterTransactionInvocable.
     */
    @Test
    public void testFilterTransactionInvocable() {
        Object obj = new PartitionTransactionInvocable(TRANSACTIONID, new CacheName("test-cache"),
                new PartitionSet(13), TransactionProcStatus.committing);
        assertPofFidelity(obj);
    }
    
    /**
     * KeyTransactionInvocable.
     */
    @Test
    public void testKeyTransactionInvocable() {
        Object obj = new KeyTransactionInvocable(TRANSACTIONID, new CacheName("test-cache"),
                new HashSet<Object>(), TransactionProcStatus.rollingback);
        assertPofFidelity(obj);
    }
    
    /**
     * TransactionStateUpdater.
     */
    @Test
    public void testTransactionStateUpdate() {
        assertPofFidelity(TransactionStateUpdater.COMMIT);
    }

    /**
     * @param expected object to check.
     */
    private void assertPofFidelity(final Object expected) {
        Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
        Object result = ExternalizableHelper.fromBinary(binary, pofContext);

        assertTrue(EqualsBuilder.reflectionEquals(expected, result));
    }

}
