package com.alipay.oceanbase.rpc.exception;

public class ObTableSessionNotExistException extends ObTableException {
    /*
     * Ob table query session not in server exception.
     */
    public ObTableSessionNotExistException() {
    }

    /*
     * Ob table query session not in server exception.
     */
    public ObTableSessionNotExistException(int errorCode) {
        super(errorCode);
    }

    /*
     * Ob table query session not in server exception.
     */
    public ObTableSessionNotExistException(String message, int errorCode) {
        super(message, errorCode);
    }

    /*
     * Ob table query session not in server exception.
     */
    public ObTableSessionNotExistException(String message) {
        super(message);
    }

    /*
     * Ob table query session not in server exception.
     */
    public ObTableSessionNotExistException(String message, Throwable cause) {
        super(message, cause);
    }

    /*
     * Ob table query session not in server exception.
     */
    public ObTableSessionNotExistException(Throwable cause) {
        super(cause);
    }

    /*
     * Is need refresh table entry.
     */
    public boolean isNeedRefreshTableEntry() {
        return false;
    }
}
