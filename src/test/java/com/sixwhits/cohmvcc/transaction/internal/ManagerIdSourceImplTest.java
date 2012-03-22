package com.sixwhits.cohmvcc.transaction.internal;

import static org.junit.Assert.assertFalse;

import org.junit.Before;
import org.junit.Test;

import com.sixwhits.cohmvcc.testsupport.AbstractLittlegridTest;
import com.sixwhits.cohmvcc.transaction.ManagerIdSource;

/**
 * Test unique generation of transaction manager ids.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class ManagerIdSourceImplTest extends AbstractLittlegridTest {
    
    private ManagerIdSource managerIdSource;

    /**
     * Create the id.
     * 
     * @throws Exception
     */
    @Before
    public void setUp() {
        
        managerIdSource = new ManagerIdSourceImpl();
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
