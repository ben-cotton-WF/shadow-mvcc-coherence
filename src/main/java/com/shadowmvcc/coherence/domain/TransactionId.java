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

package com.shadowmvcc.coherence.domain;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;

/**
 * Transaction identifier. All entries associated with a given transaction
 * must have an identifier unique to that transaction. Transaction ids must have the
 * following properties:
 * <ul>
 * <li>No two transactions may share the same id</li>
 * <li>Transaction ids must provide a time ordering</li>
 * </ul>
 * These are achieved by using an internal millisecond time stamp,
 * a transaction manager id unique to the manager producing the
 * transactions, and a sequence number to distinguish transactions
 * generated at high frequency (more than one in a millisecond interval)
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class TransactionId implements Comparable<TransactionId>, Serializable {

    private static final long serialVersionUID = 1887978179867482252L;
    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static final TransactionId END_OF_TIME =
            new TransactionId(Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);

    public static final int POF_TS = 0;
    @PortableProperty(POF_TS)
    private long timeStampMillis;

    public static final int POF_CONTEXT = 1;
    @PortableProperty(POF_CONTEXT)
    private int contextId;

    public static final int POF_SUBSEQ = 2;
    @PortableProperty(POF_SUBSEQ)
    private int subSequence;
    /**
     * Transaction id representing the dawn of time. Implicitly exists as a
     * snapshot id.
     */
    public static final TransactionId BIG_BANG = new TransactionId(0L, 0, 0);

    /**
     * Default constructor for POF use only.
     */
    public TransactionId() {
        super();
    }

    /**
     * Constructor.
     * @param timeStampMillis the timestamp for this id
     * @param contextId the unique transaction manager id
     * @param subSequence sequence number to distinguish transactions with the same manager
     * and timestamp
     */
    public TransactionId(final long timeStampMillis, final int contextId, final int subSequence) {
        super();
        this.timeStampMillis = timeStampMillis;
        this.contextId = contextId;
        this.subSequence = subSequence;
    }

    /**
     * @return the timestamp
     */
    public long getTimeStampMillis() {
        return timeStampMillis;
    }

    /**
     * @return the transaction manager id
     */
    public int getContextId() {
        return contextId;
    }

    /**
     * @return the sequence number
     */
    public int getSubSequence() {
        return subSequence;
    }

    @Override
    public int compareTo(final TransactionId o) {
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

    @Override
    public String toString() {
        return "TransactionId [" + dateFormat.format(new Date(timeStampMillis))
                + "(" + contextId + ")(" + subSequence
                + ")]";
    }


}
