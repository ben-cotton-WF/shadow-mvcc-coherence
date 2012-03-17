package com.sixwhits.cohmvcc.domain;


/**
 * The current status of a transaction.
 * 
 * @author David Whitmarsh <david.whitmarsh@sixwhits.com>
 *
 */
public enum TransactionStatus {
    open, 
    committed, 
    rolledback
}
