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
