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

package com.shadowmvcc.coherence.testsupport;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.littlegrid.ClusterMemberGroup;
import org.littlegrid.impl.DefaultClusterMemberGroupBuilder;

import com.tangosol.net.CacheFactory;

/**
 * Base class for unit tests using littlegrid. Responsible for setup and teardown
 * of the cluster, and ensuring required system properties are set.
 * 
 * Provides some useful constants
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class AbstractLittlegridTest {

    private ClusterMemberGroup cmg;
    protected static final long BASETIME = 40L * 365L * 24L * 60L * 60L * 1000L;
    protected static final String INVOCATIONSERVICENAME = "InvocationService";

    /**
     * initialise system properties.
     */
    @BeforeClass
    public static void setSystemProperties() {
        System.setProperty("pof-config-file", "mvcc-pof-config-test.xml");
        System.setProperty("tangosol.pof.enabled", "true");
        System.setProperty("littlegrid.join.timeout.milliseconds", "100");
    }

    /**
     * Set up the cluster.
     */
    @Before
    public void setUpCluster() {
        System.out.println("******setUp");
        DefaultClusterMemberGroupBuilder builder = new DefaultClusterMemberGroupBuilder();
        cmg = builder.setStorageEnabledCount(2).buildAndConfigureForStorageDisabledClient();
    }
    
    /**
     * Get the ClusterMemberGroup.
     * @return the ClusterMemberGroup
     */
    protected ClusterMemberGroup getClusterMemberGroup() {
        return cmg;
    }

    /**
     * shutdown the cluster.
     */
    @After
    public void tearDown() {
        System.out.println("******tearDown");
        CacheFactory.shutdown();
        cmg.stopAll();
    }

}
