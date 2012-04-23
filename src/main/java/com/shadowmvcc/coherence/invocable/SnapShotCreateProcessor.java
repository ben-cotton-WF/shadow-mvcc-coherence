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

package com.shadowmvcc.coherence.invocable;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import com.shadowmvcc.coherence.domain.TransactionId;
import com.shadowmvcc.coherence.exception.SnapshotCreationException;
import com.shadowmvcc.coherence.transaction.internal.SystemPropertyTimestampValidator;
import com.shadowmvcc.coherence.transaction.internal.TimestampValidator;
import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.util.InvocableMap.Entry;
import com.tangosol.util.processor.AbstractProcessor;

/**
 * Append an item to the end of a sorted set value object.
 * Returns true if successful, false if the item
 * already exists.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
@Portable
public class SnapShotCreateProcessor extends AbstractProcessor {

    private static final long serialVersionUID = 8348665851594399438L;

    private static final SortedSet<TransactionId> INITIAL_SNAPSHOTS;
    private static final TimestampValidator TIMESTAMPVALIDATOR =
            new SystemPropertyTimestampValidator();
    
    static {
        INITIAL_SNAPSHOTS = new TreeSet<TransactionId>();
        INITIAL_SNAPSHOTS.add(TransactionId.BIG_BANG);
    }
    
    @PortableProperty (0) private TransactionId value;
    
    /**
     *  Default constructor for POF use only.
     */
    public SnapShotCreateProcessor() {
        super();
    }

    /**
     * Constructor.
     * @param value value to add to the end of the set
     */
    public SnapShotCreateProcessor(final TransactionId value) {
        super();
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object process(final Entry entry) {
        
        SortedSet<TransactionId> theSet;
        
        if (!TIMESTAMPVALIDATOR.isSnapshotAgeValid(value.getTimeStampMillis())) {
            throw new SnapshotCreationException("Snapshot too recent.");
        }
        
        if (entry.isPresent()) {
            theSet = new TreeSet<TransactionId>((Collection<TransactionId>) entry.getValue());
        } else {
            theSet = INITIAL_SNAPSHOTS;
        }
        
        if (theSet.contains(value)) {
            throw new SnapshotCreationException("Snapshot already exists: " + value);
        }
        
        TransactionId oldLast = theSet.last();
        
        if (oldLast.compareTo(value) > 0) {
            throw new SnapshotCreationException("Requested snapshot " + value
                    + " older than most recent extant snapshot " + oldLast);
        }
        
        theSet.add(value);
        
        entry.setValue(theSet);
        
        return oldLast;
        
    }

}
