package com.shadowmvcc.coherence.index;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.domain.TransactionId;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;

/**
 * Test serialisation of index related classes.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class IndexSerialisationTest {

    private ConfigurablePofContext pofContext;

    /**
     * Setup POF context.
     */
    @Before
    public void setUp() {
        System.setProperty("pof-config-file", "mvcc-pof-config-test.xml");
        System.out.println("***IndexSerialisationTest setUp");
        pofContext = new ConfigurablePofContext("mvcc-pof-config.xml");
    }

    /**
     * MVCCSurfaceFilter.
     */
    @Test
    public void testMVCCSurfaceFilter() {

        System.out.println("***IndexSerialisationTest testMVCCSurfaceFilter");
        MVCCSurfaceFilter<Integer> vo = new MVCCSurfaceFilter<Integer>(
                new TransactionId(40L * 365L * 24L * 60L * 60L * 1000L + 17, 124, 457));
        assertPofFidelity(vo);
    }

    /**
     * MVCCSurfaceFilter with keys.
     */
    @Test
    public void testMVCCSurfaceFilterWithKeys() {

        System.out.println("***IndexSerialisationTest testMVCCSurfaceFilterWithKeys");
        MVCCSurfaceFilter<Integer> vo = new MVCCSurfaceFilter<Integer>(
                new TransactionId(40L * 365L * 24L * 60L * 60L * 1000L + 17, 124, 457), 
                Collections.singleton(Integer.valueOf(99)));
        assertPofFidelity(vo);
    }

    /**
     * @param expected Object to test
     */
    private void assertPofFidelity(final Object expected) {
        Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
        Object result = ExternalizableHelper.fromBinary(binary, pofContext);

        assertEquals(expected, result);
    }

}
