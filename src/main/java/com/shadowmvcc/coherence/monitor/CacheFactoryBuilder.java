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
    
    /**
     * Constructor. Starts the monitor thread.
     */
    public CacheFactoryBuilder() {
        super();
        
        monitor = new MemberTransactionMonitor();
        startMonitorThread(monitor);
        
    }
    
    /**
     * Constructor with configured monitor timeouts.
     * @param openTransactionTimeoutMillis timeout before an open transaction is rolled back
     * @param transactionCompletionTimeoutMillis timeout before a committing or rolling back transaction is completed
     * @param pollInterval how often to poll the transaction cache
     */
    public CacheFactoryBuilder(final int openTransactionTimeoutMillis,
            final int transactionCompletionTimeoutMillis, final int pollInterval) {
        super();
        
        monitor = new MemberTransactionMonitor(
                openTransactionTimeoutMillis, transactionCompletionTimeoutMillis, pollInterval);
        startMonitorThread(monitor);
    }

    /**
     * Start the monitor thread.
     * @param monitor the monitor object
     */
    protected void startMonitorThread(final Runnable monitor) {
        
        Thread memberTransactionMonitorThread = new Thread(
                monitor, "MemberTransactionMonitor");
        
        memberTransactionMonitorThread.setDaemon(true);
        
        memberTransactionMonitorThread.start();
        
    }

}
