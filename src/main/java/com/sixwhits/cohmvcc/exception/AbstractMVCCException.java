package com.sixwhits.cohmvcc.exception;

import java.io.IOException;

import com.sixwhits.cohmvcc.domain.VersionedKey;
import com.tangosol.io.pof.PofReader;
import com.tangosol.io.pof.PofWriter;
import com.tangosol.io.pof.PortableException;

/**
 * Base class for MVCC exceptions.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public abstract class AbstractMVCCException extends PortableException {

    private static final long serialVersionUID = 2338727005397469974L;
    private VersionedKey<?> key;

    /**
     * @return the version cache key the exception relates to
     */
    public VersionedKey<?> getKey() {
        return key;
    }

    /**
     * Default constructor for POF use only.
     */
    public AbstractMVCCException() {
        super();
    }

    /**
     * Constructor.
     * @param key key this exception relates to
     */
    public AbstractMVCCException(final VersionedKey<?> key) {
        super();
        this.key = key;
    }

    /**
     * Constructor.
     * @param key the key
     * @param message the message
     */
    public AbstractMVCCException(final VersionedKey<?> key, final String message) {
        super(message);
        this.key = key;
    }

    @Override
    public void readExternal(final PofReader in) throws IOException {
        super.readExternal(in);
        key = (VersionedKey<?>) in.readObject(1000);
    }

    @Override
    public void writeExternal(final PofWriter out) throws IOException {
        super.writeExternal(out);
        out.writeObject(1000, key);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
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
        AbstractMVCCException other = (AbstractMVCCException) obj;
        if (key == null) {
            if (other.key != null) {
                return false;
            }
        } else if (!key.equals(other.key)) {
            return false;
        }
        return true;
    }


}
