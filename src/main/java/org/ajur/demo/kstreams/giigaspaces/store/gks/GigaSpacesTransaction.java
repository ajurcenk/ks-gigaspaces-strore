package org.ajur.demo.kstreams.giigaspaces.store.gks;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

/**
 * GigaSpaces transaction
 */
public class GigaSpacesTransaction {

    /**
     * Transaction manager
     */
    private final PlatformTransactionManager ptm;
    private final TransactionStatus status;


    protected GigaSpacesTransaction(PlatformTransactionManager ptm, TransactionStatus status) {

        this.ptm = ptm;
        this.status = status;
    }


    public void commitTran() {

        this.ptm.commit(status);
    }

    public void rollback() {

        this.ptm.rollback(status);
    }

}
