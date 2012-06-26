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

package com.shadowmvcc.coherence.monitor;

import com.tangosol.net.DefaultCacheFactoryBuilder;

/**
 * Cache factory builder that starts a transaction monitor thread
 * to ensure transactions are cleaned up if a member dies.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class CacheFactoryBuilder extends DefaultCacheFactoryBuilder {
    
    private final MemberTransactionMonitor monitor;
    private final Thread monitorThread;
    
    /**
     * Constructor. Starts the monitor thread.
     */
    public CacheFactoryBuilder() {
        super();
        
        monitor = new MemberTransactionMonitor();
        monitorThread = startMonitorThread(monitor);
        
    }
    
    /**
     * Start the monitor thread.
     * @param monitor the monitor object
     * @return teh monitor thread
     */
    protected Thread startMonitorThread(final Runnable monitor) {
        
        Thread memberTransactionMonitorThread = new Thread(
                monitor, "MemberTransactionMonitor");
        
        memberTransactionMonitorThread.setDaemon(true);
        
        memberTransactionMonitorThread.start();
        
        return memberTransactionMonitorThread;
        
    }
    
    /**
     * Stop the monitor thread.
     */
    public void stopMonitorThread() {
        monitor.stop();
        try {
            monitorThread.join();
        } catch (InterruptedException e) {
        }
    }

}
