package com.sixwhits.cohmvcc.invocable;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.cache.internal.UnconditionalPutProcessor;
import com.sixwhits.cohmvcc.domain.IsolationLevel;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.net.partition.PartitionSet;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.aggregator.Count;
import com.tangosol.util.filter.AlwaysFilter;
import com.tangosol.util.processor.ConditionalPut;

/**
 * Serialisation tests for the invocable package.
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class InvocableSerialisationTest {

    private ConfigurablePofContext pofContext;

    /**
     * Initialise the POF context.
     */
    @Before
    public void setUp() {
        pofContext = new ConfigurablePofContext("mvcc-pof-config.xml");
    }

    /**
     * MVCCEntryProcessorWrapper.
     */
    @Test
    public void testMVCCEntryProcessorWrapper() {

        MVCCEntryProcessorWrapper<String, Object> wrapper = new MVCCEntryProcessorWrapper<String, Object>(
                new TransactionId(40L * 365L * 24L * 60L * 60L * 1000L + 17, 124, 457), 
                new ConditionalPut(AlwaysFilter.INSTANCE, "a test value"), 
                IsolationLevel.serializable, false, new CacheName("acachename"));

        assertPofFidelity(wrapper);
    }

    /**
     * MVCCReadOnlyEntryProcessorWrapper.
     */
    @Test
    public void testMVCCReadOnlyEntryProcessorWrapper() {

        Object wrapper = new MVCCReadOnlyEntryProcessorWrapper<String, Object>(
                new TransactionId(40L * 365L * 24L * 60L * 60L * 1000L + 17, 124, 457), 
                new ConditionalPut(AlwaysFilter.INSTANCE, "a test value"), 
                IsolationLevel.serializable, new CacheName("acachename"));

        assertPofFidelity(wrapper);
    }

    /**
     * EntryProcessorInvoker.
     */
    @Test
    public void testEntryProcessorInvoker() {

        Object obj = new EntryProcessorInvoker<String, Object>(new CacheName("acachename"), AlwaysFilter.INSTANCE, 
                new TransactionId(40L * 365L * 24L * 60L * 60L * 1000L + 17, 124, 457), 
                new ConditionalPut(AlwaysFilter.INSTANCE, "a test value"));

        assertPofFidelity(obj);
    }

    /**
     * EntryProcessorInvokerResult.
     */
    @Test
    public void testEntryProcessorInvokerResult() {

        Object obj = new EntryProcessorInvokerResult<String, Object>(
                new PartitionSet(5), new HashMap<String, Object>(), new HashMap<String, VersionedKey<String>>());

        assertPofFidelity(obj);
    }

    /**
     * UnconditionalPutProcessor.
     */
    @Test
    public void testUnconditionalPut() {
        Object obj = new UnconditionalPutProcessor("Test value", true);
        assertPofFidelity(obj);
    }

    /**
     * ParallelAwareAggregatorWrapper.
     */
    @Ignore
    @Test
    public void testParallelAwareAggregatorWrapper() {
        Object obj = new ParallelAwareAggregatorWrapper(new Count());
        assertPofFidelity(obj);
    }

    /**
     * AggregatorWrapper.
     */
    @Ignore
    @Test
    public void testAggregatorWrapper() {
        Object obj = new AggregatorWrapper(new Count());
        assertPofFidelity(obj);
    }


    /**
     * @param expected object to test.
     */
    private void assertPofFidelity(final Object expected) {
        Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
        Object result = ExternalizableHelper.fromBinary(binary, pofContext);

        assertTrue(EqualsBuilder.reflectionEquals(expected, result));
    }

}
