package com.alipay.oceanbase.rpc.exception;

public class ObTableSchemaVersionMismatchException extends ObTableException {
    /*
     * Ob table schema version mismatch exception.
     */
    public ObTableSchemaVersionMismatchException() {
    }

    /*
     * Ob table schema version mismatch exception with error code.
     */
    public ObTableSchemaVersionMismatchException(int errorCode) {
        super(errorCode);
    }

    /*
     * Ob table schema version mismatch exception with message and error code.
     */
    public ObTableSchemaVersionMismatchException(String message, int errorCode) {
        super(message, errorCode);
    }

    /*
     * Ob table schema version mismatch exception with message.
     */
    public ObTableSchemaVersionMismatchException(String message) {
        super(message);
    }

    /*
     * Ob table schema version mismatch exception with message and cause.
     */
    public ObTableSchemaVersionMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

    /*
     * Ob table schema version mismatch exception with case.
     */
    public ObTableSchemaVersionMismatchException(Throwable cause) {
        super(cause);
    }
}
