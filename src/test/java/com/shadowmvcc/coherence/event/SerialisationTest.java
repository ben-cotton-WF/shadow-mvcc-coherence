package com.shadowmvcc.coherence.event;

import static com.shadowmvcc.coherence.domain.IsolationLevel.readCommitted;
import static junit.framework.Assert.assertTrue;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.cache.CacheName;
import com.shadowmvcc.coherence.domain.IsolationLevel;
import com.shadowmvcc.coherence.domain.TransactionId;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;
import com.tangosol.util.filter.AlwaysFilter;

/**
 * event serialisation test.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class SerialisationTest {

    private ConfigurablePofContext pofContext;

    /**
     * Set up POF context.
     */
    @Before
    public void setUp() {
        pofContext = new ConfigurablePofContext("mvcc-pof-config.xml");
    }

    /**
     * Test MVCCEventTransformer.
     */
    @Test
    public void testMVCCEventTransformer() {

        Object vo = new MVCCEventTransformer<Object, Object>(readCommitted, 
                new TransactionId(40L * 365L * 24L * 60L * 60L * 1000L + 17, 124, 457), new CacheName("test"));
        assertPofFidelity(vo);
    }
    
    /**
     * Test MVCCEventFilter.
     */
    @Test
    public void testMVCCEventFilter() {

        Object vo = new MVCCEventFilter<Object>(
                IsolationLevel.readCommitted, AlwaysFilter.INSTANCE, new CacheName("xx"), null);
        assertPofFidelity(vo);
    }

    /**
     * @param expected object to test
     */
    private void assertPofFidelity(final Object expected) {
        Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
        Object result = ExternalizableHelper.fromBinary(binary, pofContext);

        assertTrue(EqualsBuilder.reflectionEquals(expected, result));
    }

}
