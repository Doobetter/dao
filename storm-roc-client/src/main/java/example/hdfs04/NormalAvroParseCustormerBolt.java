package example.hdfs04;

import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.roc.configuration.BoltConfiguration;
import org.roc.customer.CustomerETLBolt;
import org.roc.utils.AvroUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NormalAvroParseCustormerBolt extends CustomerETLBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//private User user = new User();

	public NormalAvroParseCustormerBolt(BoltConfiguration bltConf) {
		super(bltConf);
	}

	public static Logger logger = LoggerFactory.getLogger(NormalAvroParseCustormerBolt.class);
	//Important！！ 这个类中的变量都要在setup中初始化
	private AvroUtil avroUtil =null;

	
	@Override
	protected void setup(Map stormConf) {
		avroUtil = new AvroUtil(User.class);
	}

	@Override
	public void execute(Tuple input) {
		//can change the process staff
		super.execute(input);
	}

	@Override
	public Object getRecord(Tuple tuple) {	
		User user = new User();
		String[] strs;
		try{
				logger.error("WOW");
				String str = getInput(tuple,0);
				logger.error("Can we get a String from the tuple?"+str);
				logger.error("Tuple value:"+str);
				strs = str.split(",");
				user.setName(strs[0]);
				user.setFavoriteColor(strs[1]);
				user.setFavoriteNumber(Integer.parseInt(strs[2]));
		}
		catch(Exception ex)
		{
			logger.error(ex.getMessage());
			logger.error("Object to string:"+tuple.getMessageId().toString());
		}
		return user;
	}
	
	public String getInput(Tuple tuple,int index) {
		
		Object obj = tuple.getValue(index);
		String record = null;
		if(obj instanceof String){
			record = (String)obj;
		}
		else if(obj instanceof byte[]){
			record = Bytes.toString((byte[])obj);
		}else{
			logger.warn("Tuple value is not String or bytes");
			this.collector.fail(tuple);
		}
		return record;
	}
	
	@Override
	public void emitTuple(Tuple anchor, Object record)  {
		
		this.collector.emit(anchor, new Values(avroUtil.toBytes(record)));
		
		//try{
		/*if (record != null) {
			
			//byte[] bdBytes = AvroNewUtil.getByteArrayFromAvroObj((User)record, User.class);
			//this.collector.emit(anchor, new Values(bdBytes));
			//this.collector.ack(anchor);
			logger.error("Message has been sent:"+((User)record).getName()+"|"+((User)record).getFavoriteColor()+"|"+((User)record).getFavoriteNumber());
		} else {
			this.collector.fail(anchor);
		}*/
		//}
//		catch(IOException ioe)
//		{
//			logger.equals(ioe.getMessage());
//		}
	}


}
