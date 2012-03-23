package com.sixwhits.cohmvcc.monitor;

import com.tangosol.net.ConfigurableCacheFactory;
import com.tangosol.net.DefaultCacheFactoryBuilder;
import com.tangosol.run.xml.XmlElement;

/**
 * Cache factory builder that starts a transaction monitor thread
 * to ensure transactions are cleaned up if a member dies.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class CacheFactoryBuilder extends DefaultCacheFactoryBuilder {

    @Override
    protected ConfigurableCacheFactory buildFactory(final XmlElement xmlConfig,
            final ClassLoader loader) {
        
        ConfigurableCacheFactory factory = super.buildFactory(xmlConfig, loader);
        
        //TODO set configuration options
        Thread memberTransactionMonitorThread = new Thread(
                new MemberTransactionMonitor(30000, 10000, 5000), "MemberTransactionMonitor");
        
        memberTransactionMonitorThread.setDaemon(true);
        
        memberTransactionMonitorThread.start();
        
        return factory;
    }

}
