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

import static org.junit.Assert.assertFalse;

import org.junit.Before;
import org.junit.Test;

import com.shadowmvcc.coherence.testsupport.AbstractLittlegridTest;
import com.shadowmvcc.coherence.transaction.ManagerCache;

/**
 * Test unique generation of transaction manager ids.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class ManagerIdSourceImplTest extends AbstractLittlegridTest {
    
    private ManagerCache managerIdSource;

    /**
     * Create the id.
     * 
     * @throws Exception
     */
    @Before
    public void setUp() {
        
        managerIdSource = new ManagerCacheImpl();
    }

    /**
     * Get a couple of ids and check they aren't the same.
     */
    @Test
    public void testGetManagerId() {
        
        int id1 = managerIdSource.getManagerId();
        
        int id2 = managerIdSource.getManagerId();
        
        assertFalse(id1 == id2);
    }

}
