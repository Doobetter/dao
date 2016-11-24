package org.roc.hdfs.bolt.rules;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveAction {
	private static final Logger LOG = LoggerFactory.getLogger(ArchiveAction.class);


	public void execute(FileSystem fileSystem, Path filePath)
			throws IOException {

		// 如果文件大小为0，删除
		long size = fileSystem.getContentSummary(filePath).getLength();
		if (size <= 0) {
			fileSystem.delete(filePath, false);
		}
		// 重命名，归档
		Path destPath = new Path(filePath.getParent(), filePath.getName()
				.replaceAll(".tmp", ""));
		LOG.info("Moving file {} to {}", filePath, destPath);
		boolean success = fileSystem.rename(filePath, destPath);
		return;
	}
}