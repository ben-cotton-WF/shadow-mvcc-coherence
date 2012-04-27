package com.shadowmvcc.coherence.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration implementation using a properties file and/or
 * system properties.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class PropertyConfiguration implements Configuration {

    public static final long SNAPSHOT_SAFETY_MARGIN = 60000;
    public static final long DEFAULT_MAXIMUMTRANSACTIONAGE = 300000;
    public static final long DEFAULT_MINIMUMSNAPSHOTAGE = 3600000;
    public static final long DEFAULT_OPENTRANSACTIONTIMEOUT = 60000;
    public static final long DEFAULT_TRANSACTIONCOMPLETIONTIMEOUT = 20000;
    public static final long DEFAULT_TRANSACTIONPOLLINTERVAL = 30000;
    public static final String DEFAULT_INVOCATIONSERVICENAME = "InvocationService";
    private final long maximumTransactionAge;
    private final long minimumSnapshotAge;
    private final String invocationServiceName;
    private final long openTransactionTimeout;
    private final long transactionCompletionTimeout;
    private final long transactionPollInterval;
    
    static final String PROPERTYFILENAME = "shadowmvcc.properties";
    static final String PROP_MINSNAPSHOTAGE = "minsnapshotage";
    static final String PROP_MAXTRANSACTIONAGE = "maxtransactionage";
    static final String PROP_OPENTRANSACTIONTIMEOUT = "opentransactiontimeout";
    static final String PROP_TRANSACTIONCOMPLETIONTIMEOUT = "transactioncompletiontimeout";
    static final String PROP_TRANSACTIONPOLLINTERVAL = "transactionpollinterval";
    static final String PROP_INVOCATIONSERVICENAME = "invocationservicename";
    static final String SYSTEM_PROPERTY_PREFIX = "shadowmvcc.";
    
    /**
     * Construct a configuration. Each configured parameter is set in order of precedence by the system property,
     * property from shadowmvcc.properties, or hard-wired default.
     * 
     */
    public PropertyConfiguration() {
        super();
        
        Properties properties = new Properties();
        InputStream propertyFile = this.getClass().getClassLoader().getResourceAsStream(PROPERTYFILENAME);
        
        if (propertyFile != null) {
            try {
                properties.load(propertyFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        invocationServiceName = loadProperty(properties, PROP_INVOCATIONSERVICENAME, DEFAULT_INVOCATIONSERVICENAME);
        maximumTransactionAge = loadLongProperty(properties, PROP_MAXTRANSACTIONAGE, DEFAULT_MAXIMUMTRANSACTIONAGE);
        minimumSnapshotAge = loadLongProperty(properties, PROP_MINSNAPSHOTAGE, DEFAULT_MINIMUMSNAPSHOTAGE);
        openTransactionTimeout = loadLongProperty(
                properties, PROP_OPENTRANSACTIONTIMEOUT, DEFAULT_OPENTRANSACTIONTIMEOUT);
        transactionCompletionTimeout = loadLongProperty(
                properties, PROP_TRANSACTIONCOMPLETIONTIMEOUT, DEFAULT_TRANSACTIONCOMPLETIONTIMEOUT);
        transactionPollInterval = loadLongProperty(
                properties, PROP_TRANSACTIONPOLLINTERVAL, DEFAULT_TRANSACTIONPOLLINTERVAL);
        
        long minminSnapshotAge = maximumTransactionAge + openTransactionTimeout
                + transactionCompletionTimeout + transactionPollInterval + SNAPSHOT_SAFETY_MARGIN;
        
        if (minimumSnapshotAge < minminSnapshotAge) {
            throw new RuntimeException("miniumSnapshotAge " + minimumSnapshotAge
                    + " is too small, minimum value is " + minminSnapshotAge);
        }
        
    }

    /**
     * Load a string property from properties file, system property or default.
     * @param properties file properties
     * @param propertyName property name
     * @param defaultValue default value
     * @return the value to use
     */
    private String loadProperty(final Properties properties, final String propertyName, final String defaultValue) {
        
        String result = getProperty(properties, propertyName);
        
        return result == null ? defaultValue : result;
    }
    
    /**
     * Load a Long property from properties file, system property or default.
     * @param properties file properties
     * @param propertyName property name
     * @param defaultValue default value
     * @return the value to use
     */
    private Long loadLongProperty(final Properties properties, final String propertyName, final long defaultValue) {
        
        String stringProperty = getProperty(properties, propertyName);
        
        return stringProperty == null ? defaultValue : Long.parseLong(stringProperty);
    }
    
    /**
     * Get a property from the properties file or system property.
     * @param properties file properties
     * @param propertyName property name
     * @return value if set in either place, or null
     */
    private String getProperty(final Properties properties, final String propertyName) {
        
        String systemProperty = SYSTEM_PROPERTY_PREFIX + propertyName;
        
        String result = System.getProperty(systemProperty);
        
        if (result == null) {
            result = properties.getProperty(propertyName);
        }
        
        return result;
    }

    @Override
    public long getMaximumTransactionAge() {
        return maximumTransactionAge;
    }

    @Override
    public long getMinimumSnapshotAge() {
        return minimumSnapshotAge;
    }

    @Override
    public String getInvocationServiceName() {
        return invocationServiceName;
    }

    @Override
    public long getOpenTransactionTimeout() {
        return openTransactionTimeout;
    }

    @Override
    public long getTransactionCompletionTimeout() {
        return transactionCompletionTimeout;
    }

    @Override
    public long getTransactionPollInterval() {
        return transactionPollInterval;
    }

}
