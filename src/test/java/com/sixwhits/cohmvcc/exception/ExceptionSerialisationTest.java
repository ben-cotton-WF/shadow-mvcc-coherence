package com.sixwhits.cohmvcc.exception;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.sixwhits.cohmvcc.domain.TransactionId;
import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;

/**
 * Serialisation tests.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class ExceptionSerialisationTest {

    private static final long BASETIME = 40L * 365L * 24L * 60L * 60L * 1000L;
    private ConfigurablePofContext pofContext;

    /**
     * Setup POF context.
     */
    @Before
    public void setUp() {
        pofContext = new ConfigurablePofContext("mvcc-pof-config.xml");
    }

    /**
     * Future read.
     */
    @Test
    public void testTFutureReadException() {

        FutureReadException vo = new FutureReadException(
                new VersionedKey<Integer>(99, new TransactionId(BASETIME, 0, 0)));
        assertPofFidelity(vo);
    }
    /**
     * Uncommitted read.
     */
    @Test
    public void testUncommittedReadException() {

        UncommittedReadException vo = new UncommittedReadException(
                new VersionedKey<Integer>(99, new TransactionId(BASETIME, 0, 0)));
        assertPofFidelity(vo);
    }


    /**
     * @param expected value to check
     */
    private void assertPofFidelity(final Object expected) {
        Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
        Object result = ExternalizableHelper.fromBinary(binary, pofContext);

        assertEquals(expected, result);

    }

}
