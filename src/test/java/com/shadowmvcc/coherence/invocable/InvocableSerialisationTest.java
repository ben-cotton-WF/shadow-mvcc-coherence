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

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Set;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.cache.internal.UnconditionalPutProcessor;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.domain.VersionCacheKey;
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

        Object obj = new EntryProcessorInvokerResult<String, Object>(new PartitionSet(5), 
                new HashMap<String, Object>(), new HashMap<String, VersionCacheKey<String>>(),
                new HashMap<CacheName, Set<?>>());

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
