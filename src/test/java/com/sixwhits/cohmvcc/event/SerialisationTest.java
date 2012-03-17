package com.sixwhits.cohmvcc.event;

import static com.sixwhits.cohmvcc.domain.IsolationLevel.readCommitted;
import static junit.framework.Assert.assertTrue;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.junit.Before;
import org.junit.Test;

import com.sixwhits.cohmvcc.cache.CacheName;
import com.sixwhits.cohmvcc.domain.TransactionId;
import com.tangosol.io.pof.ConfigurablePofContext;
import com.tangosol.util.Binary;
import com.tangosol.util.ExternalizableHelper;

public class SerialisationTest {

    private ConfigurablePofContext pofContext;

    @Before
    public void setUp() throws Exception {
        pofContext = new ConfigurablePofContext("mvcc-pof-config.xml");
    }

    @Test
    public void testMVCCEventTransformer() {

        Object vo = new MVCCEventTransformer<Object, Object>(readCommitted, 
                new TransactionId(40L * 365L * 24L * 60L * 60L * 1000L + 17, 124, 457), new CacheName("test"));
        assertPofFidelity(vo);
    }

    private void assertPofFidelity(Object expected) {
        Binary binary = ExternalizableHelper.toBinary(expected, pofContext);
        Object result = ExternalizableHelper.fromBinary(binary, pofContext);

        assertTrue(EqualsBuilder.reflectionEquals(expected, result));
    }

}
