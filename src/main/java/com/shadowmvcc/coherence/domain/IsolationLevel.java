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

/**
 * Transaction isolation levels, follows ISO convention with extensions.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public enum IsolationLevel {
    /**
     * Reads uncommitted values. Will never wait for commit
     */
    readUncommitted, 
    /**
     * Read only committed values. Wait for uncommitted entries
     */
    readCommitted, 
    /**
     * Read committed and insert a read marker to prevent updates with an earlier timestamp.
     */
    repeatableRead, 
    /**
     * Repeatable read, and record filter criteria against the timestamp to prevent updates with
     * an earlier timestamp that would match the filter criteria.
     */
    serializable, 
    /**
     * Update only operation with out reading. Allows the optimisation of not having to check
     * for uncommitted changes
     */
    readProhibited, 
//    readCommittedNoWait, 
//    eternal
}
