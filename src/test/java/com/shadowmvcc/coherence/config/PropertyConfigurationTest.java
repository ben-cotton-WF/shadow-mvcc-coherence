package com.shadowmvcc.coherence.config;

import static junit.framework.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test instantiation of the configuration class.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public class PropertyConfigurationTest {

    /**
     * Set the system property.
     */
    @BeforeClass
    public static void initSystemProperty() {

        System.setProperty(PropertyConfiguration.SYSTEM_PROPERTY_PREFIX
                + PropertyConfiguration.PROP_MAXTRANSACTIONAGE, "2500");
        
    }
    /**
     * Test instantiation. 
     */
    @Test
    public void testBuildConfiguration() {
        
        Configuration configuration = new PropertyConfiguration();
        
        assertConfiguration(configuration);
        
    }
    
    /**
     * Test the factory.
     */
    @Test
    public void testConfigFactory() {
        assertConfiguration(ConfigurationFactory.getConfiguraration());
    }
    
    /**
     * Check the configuration is as expected.
     * 
     * @param configuration the configuration to check
     */
    private void assertConfiguration(final Configuration configuration) {
        
        // from system property
        assertEquals(configuration.getMaximumTransactionAge(), 2500);
        
        // from hard coded default
        assertEquals(configuration.getMinimumSnapshotAge(), PropertyConfiguration.DEFAULT_MINIMUMSNAPSHOTAGE);
        assertEquals(configuration.getInvocationServiceName(),
                PropertyConfiguration.DEFAULT_INVOCATIONSERVICENAME);
        
        // from properties file
        assertEquals(configuration.getOpenTransactionTimeout(), 60000);
        assertEquals(configuration.getTransactionCompletionTimeout(), 10000);
        assertEquals(configuration.getTransactionPollInterval(), 5000);
        
    }
    
}
