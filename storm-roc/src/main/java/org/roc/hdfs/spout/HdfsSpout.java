package org.roc.hdfs.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;

public class HdfsSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		super.ack(msgId);
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		super.fail(msgId);
	}

	static class HdfsMessageId{
		public String filePath = null;
		public long position;
	}
}
