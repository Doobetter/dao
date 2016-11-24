/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.roc.hdfs.bolt.rules;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * File rotation policy that will rotate files when a certain file size is
 * reached.
 * 
 * For example:
 * 
 * <pre>
 * // rotate when files reach 5MB
 * FileSizeRotationPolicy policy = new FileSizeRotationPolicy(5.0, Units.MB);
 * </pre>
 * 
 */
public class FileSizeRotationPolicy implements FileRotationPolicy {
	private static final Logger LOG = LoggerFactory
			.getLogger(FileSizeRotationPolicy.class);

	public static enum Units {

		KB((long) Math.pow(2, 10)), MB((long) Math.pow(2, 20)), GB((long) Math
				.pow(2, 30)), TB((long) Math.pow(2, 40));

		private long byteCount;

		private Units(long byteCount) {
			this.byteCount = byteCount;
		}

		public long getByteCount() {
			return byteCount;
		}
	}

	private long maxBytes;


	public FileSizeRotationPolicy(long fileSizeBytes) {

		this.maxBytes = fileSizeBytes;
	}

	public FileSizeRotationPolicy(float count, Units units) {
		this.maxBytes = (long) (count * units.getByteCount());
	}


	// if true , file can be rotated
	@Override
	public boolean mark(FileSystem fs, Path path, FileNameFormat format) {
		long size = 0L;
		try {
			size = fs.getContentSummary(path).getLength();
		} catch (IOException e) {
			LOG.warn("Error when get file's length  ", e);
			return false;
		}
		if (size >= this.maxBytes) {
			return true;
		} else {
			return false;
		}
	}





}
