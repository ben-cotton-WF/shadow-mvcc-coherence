package com.shadowmvcc.coherence.config;

/**
 * Initialise the cluster time provider instance.
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@SuppressWarnings("unchecked")
public final class ClusterTimeProviderFactory {
    
    /**
     * Private constructor to prevent instantiation.
     */
    private ClusterTimeProviderFactory() {
    }
    
    private static final ClusterTimeProvider INSTANCE;
    
    static {
        
        Class<?> providerClass = ClusterTimeProviderImpl.class;
        
        String providerClassName = System.getProperty("shadowmvcc.timeproviderclass");
        if (providerClassName != null) {
            try {
                providerClass = (Class<ClusterTimeProvider>) Class.forName(providerClassName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            INSTANCE = (ClusterTimeProvider) providerClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }        
    }
    
    /**
     * Get the global cluster time provider instance.
     * @return the cluster time provider
     */
    public static ClusterTimeProvider getInstance() {
        return INSTANCE;
    }
}
