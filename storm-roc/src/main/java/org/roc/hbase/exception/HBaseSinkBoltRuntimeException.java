package org.roc.hbase.exception;

public class HBaseSinkBoltRuntimeException extends RuntimeException {
	public HBaseSinkBoltRuntimeException(Throwable cause) {
		super(cause);
	}

	public HBaseSinkBoltRuntimeException(String message) {
		super(message);
	}

	public HBaseSinkBoltRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

}
