package org.roc.hdfs.bolt.rules;


import java.io.Serializable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public interface FileRotationPolicy extends Serializable {

	boolean mark(FileSystem fs, Path path,FileNameFormat format);
}
