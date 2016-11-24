package org.roc.hdfs.bolt.rules;



import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TimedRotationPolicy implements FileRotationPolicy {
	private static final long serialVersionUID = 1L;


	public static enum TimeUnit {

		SECONDS((long) 1000), MINUTES((long) 1000 * 60), HOURS(
				(long) 1000 * 60 * 60), DAYS((long) 1000 * 60 * 60 * 24);

		private long milliSeconds;

		private TimeUnit(long milliSeconds) {
			this.milliSeconds = milliSeconds;
		}

		public long getMilliSeconds() {
			return milliSeconds;
		}
	}

	private long interval;


	public TimedRotationPolicy(float count, TimeUnit units) {
		this.interval = (long) (count * units.getMilliSeconds());
	}

	public TimedRotationPolicy(long millis, long maxCheck) {
		this.interval = millis;

	}

	public TimedRotationPolicy(long millis) {
		this.interval = millis;

	}

	

	public long getInterval() {
		return this.interval;
	}

	
	@Override
	public boolean mark(FileSystem fs, Path path, FileNameFormat format) {
		String fileName = path.getName();
		// 依赖于文件的命名规范
		long createTime = Long.parseLong(format.getTimeStamp(fileName));

		if (System.currentTimeMillis() - createTime > this.interval) {
			return true;
		} else {
			return false;
		}

	}
}
