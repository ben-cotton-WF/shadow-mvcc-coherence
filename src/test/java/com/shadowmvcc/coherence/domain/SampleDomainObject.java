package com.shadowmvcc.coherence.domain;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * A sample object used in tests.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class SampleDomainObject {
    public static final int POF_INTV = 0;
    @PortableProperty(POF_INTV)
    private int intValue;
    public static final int POF_STRV = 1;
    @PortableProperty(POF_STRV)
    private String stringValue;


    /**
     *  Default constructor for POF use only.
     */
    public SampleDomainObject() {
        super();
    }
    /**
     * @param intValue int value
     * @param stringValue string value
     */
    public SampleDomainObject(final int intValue, final String stringValue) {
        super();
        this.intValue = intValue;
        this.stringValue = stringValue;
    }
    /**
     * @return int value
     */
    public int getIntValue() {
        return intValue;
    }
    /**
     * @param intValue int value
     */
    public void setIntValue(final int intValue) {
        this.intValue = intValue;
    }
    /**
     * @return string value
     */
    public String getStringValue() {
        return stringValue;
    }
    /**
     * @param stringValue string value
     */
    public void setStringValue(final String stringValue) {
        this.stringValue = stringValue;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + intValue;
        result = prime * result
                + ((stringValue == null) ? 0 : stringValue.hashCode());
        return result;
    }
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SampleDomainObject other = (SampleDomainObject) obj;
        if (intValue != other.intValue) {
            return false;
        }
        if (stringValue == null) {
            if (other.stringValue != null) {
                return false;
            }
        } else if (!stringValue.equals(other.stringValue)) {
            return false;
        }
        return true;
    }

}
