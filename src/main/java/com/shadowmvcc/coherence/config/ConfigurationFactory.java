package com.shadowmvcc.coherence.config;

/**
 * Holder for the global configuration singleton.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public final class ConfigurationFactory {

    private static final Configuration INSTANCE = new PropertyConfiguration();
    
    /**
     * Private constructor to prevent instantiation.
     */
    private ConfigurationFactory() {
    }
    /**
     * Get the global static configuration instance.
     * @return the configuration instance
     */
    public static Configuration getConfiguraration() {
        return INSTANCE;
    }

}
