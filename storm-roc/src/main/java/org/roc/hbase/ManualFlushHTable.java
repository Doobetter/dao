package org.roc.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

public class ManualFlushHTable extends HTable {

	public ManualFlushHTable(Configuration conf, String string) throws IOException {
		super(conf, string);
	}

	public void clearWriteBuffer() {
		this.currentWriteBufferSize = 0;
		this.writeAsyncBuffer.clear();
	}

}
