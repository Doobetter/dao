package org.roc.exception;

public class HBasePutException extends Exception {
	public HBasePutException(Throwable cause) {
		super(cause);
	}

	public HBasePutException(String message) {
		super(message);
	}

	public HBasePutException(String message, Throwable cause) {
		super(message, cause);
	}

}
