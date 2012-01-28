package com.sixwhits.cohmvcc.domain;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

@Portable
public class TransactionId implements Comparable<TransactionId>, Serializable {
    
	private static final long serialVersionUID = 1887978179867482252L;
	
	public static final int POF_TS = 0;
	@PortableProperty(POF_TS)
    private long timeStampMillis;
	
	public static final int POF_CONTEXT = 1;
	@PortableProperty(POF_CONTEXT)
    private int contextId;
	
	public static final int POF_SUBSEQ = 2;
	@PortableProperty(POF_SUBSEQ)
    private int subSequence;

	public TransactionId() {
        super();
	}
	
    public TransactionId(long timeStampMillis, int contextId, int subSequence) {
        super();
        this.timeStampMillis = timeStampMillis;
        this.contextId = contextId;
        this.subSequence = subSequence;
    }
    
    public long getTimeStampMillis() {
        return timeStampMillis;
    }

    public int getContextId() {
        return contextId;
    }

    public int getSubSequence() {
        return subSequence;
    }

    public int compareTo(TransactionId o) {
        if (timeStampMillis < o.getTimeStampMillis()) {
            return -1;
        } else if (timeStampMillis == o.getTimeStampMillis()) {
            if (contextId < o.getContextId()) {
                return -1;
            } else if (contextId == o.getContextId()) {
                if (subSequence < o.getSubSequence()) {
                    return -1;
                } else if (subSequence == o.getSubSequence()) {
                    return 0;
                }
            }
        }
        return 1;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + contextId;
        result = prime * result + subSequence;
        result = prime * result
                + (int) (timeStampMillis ^ (timeStampMillis >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TransactionId other = (TransactionId) obj;
        if (contextId != other.contextId) {
            return false;
        }
        if (subSequence != other.subSequence) {
            return false;
        }
        if (timeStampMillis != other.timeStampMillis) {
            return false;
        }
        return true;
    }

    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	@Override
	public String toString() {
		return "TransactionId [" + dateFormat.format(new Date(timeStampMillis))
				+ "(" + contextId + ")(" + subSequence
				+ ")]";
	}

    
}
