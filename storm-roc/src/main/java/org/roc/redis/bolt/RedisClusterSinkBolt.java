package org.roc.redis.bolt;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.roc.configuration.BoltConfiguration;
import org.roc.configuration.BoltConfiguration.BoltConfConstant;
import org.roc.configuration.Relation.Dependency;
import org.roc.redis.KryoRedisTuple;
import org.roc.redis.KryoRedisTuple.ValueType;
import org.roc.redis.KryoRedisTuple.WriteMethod;
import org.roc.utils.KryoUtil;

/**
 * Redis Cluster Bolt
 *  使用Jedis API
 *  输入tuple的类型为KryoRedisTuple 或者其 bytes
 * @author LC
 *
 */
public class RedisClusterSinkBolt  implements IRichBolt {
	public static Logger logger = LoggerFactory.getLogger(RedisClusterSinkBolt.class);

	private BoltConfiguration boltConf ;
	private String confJSON ;
	// 参数文件中可配置的参数
	// host1:port1,host2:port2
	private String hostAndPort;
	private int redisCommandTimeout;
	private int redisMaxRedirections;
	HashSet<String> dependencyStreamIDSet = new HashSet<String>();
	//private int maxActive;
	//private int maxIdle;
	//private int maxWait;
	//private boolean testOnBorrow;
	
	// redis
	private JedisCluster jc = null;
	// storm 
	private OutputCollector collector = null;
	
	KryoUtil kryoUtil = null;
	
	public RedisClusterSinkBolt(){}
	public RedisClusterSinkBolt(BoltConfiguration boltConf){
		this.boltConf = boltConf;
		settingByConfig();
	}
	public RedisClusterSinkBolt(String json){
		this.confJSON = json;
	}
	
	private void settingByConfig(){
		Properties props = this.boltConf.getProperties();
		this.hostAndPort = props.getProperty(BoltConfConstant.REDIS_HOSTANDPORT);
		this.redisCommandTimeout = Integer.parseInt(props.getProperty(BoltConfConstant.REDIS_COMMAND_TIMEOUT));
		this.redisMaxRedirections = Integer.parseInt(props.getProperty(BoltConfConstant.REDIS_MAX_REDIRECTIONS));
		
		for (Dependency d : this.boltConf.getDependencies()) {
			this.dependencyStreamIDSet.add(d.getStreamID());
		}
		this.confJSON =this.boltConf.toJSON();
		
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		String [] hPs= this.hostAndPort.split(",");
		for (String hPStr :hPs){
			String [] hP = hPStr.split(":");
			jedisClusterNodes.add(new HostAndPort(hP[0], Integer.parseInt(hP[1])));
		}
		/*
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMaxTotal(this.maxActive);
		poolConfig.setMaxIdle(this.maxIdle);
		poolConfig.setMaxWaitMillis(this.maxWait);
		poolConfig.setTestOnBorrow(this.testOnBorrow);
		JedisCluster jc = new JedisCluster(jedisClusterNodes,poolConfig);
		*/
		this.jc = new JedisCluster(jedisClusterNodes,this.redisCommandTimeout,this.redisMaxRedirections);
		this.kryoUtil = new KryoUtil(KryoRedisTuple.class);
		
	}

	@Override
	public void execute(Tuple input) {
		if(this.dependencyStreamIDSet.contains(input.getSourceStreamId())){
			
			Object obj = input.getValue(0);
			KryoRedisTuple redisTuple = null;
			if(obj instanceof byte[]){
				redisTuple = kryoUtil.toObject((byte[])obj, KryoRedisTuple.class);
			}
			else if(obj instanceof KryoRedisTuple){
				
				redisTuple = (KryoRedisTuple)obj;
			}
			else{
				logger.error("Redis Bolt's input must be KryoRedisTuple or its' bytes ");
			}
			
			if(redisTuple!=null){
				try{
					String key = redisTuple.getKey();
					Object value = redisTuple.getValue();
					ValueType type = redisTuple.getType();
					WriteMethod method = redisTuple.getMethod() ;
					int expireSeconds = redisTuple.getExpireSeconds();
					
					if(ValueType.STRING == type){
						//字符串
						
						if(WriteMethod.INCR == method){
							jc.incr(key);
						}
						else if (WriteMethod.INCR_BY == method){
							jc.incrBy(key, (Long)value);
						}
						else if (WriteMethod.SET== method){
							jc.set( key, (String)value );
						}
						
					}
					else if(ValueType.LIST == type){
						if(WriteMethod.LPUSH == method){
							// list left 
							if(value instanceof String){
								jc.lpush(key, (String)value);
							}
							else if(value instanceof String[]){
								jc.lpush( key, (String[])value );	
							}
							
						}else if(WriteMethod.RPUSH == method){
							// list right
							if(value instanceof String){
								jc.rpush(key, (String)value);
							}
							else if(value instanceof String[]){
								jc.rpush( key, (String[])value );	
							}
						}
					}
					else if(ValueType.HASH == type){
						//散列表（HashMap）
						if(WriteMethod.HMSET == method){
							jc.hmset( key, (Map<String,String>)value );
						}
						
					}
					else if(ValueType.SET == type){
						// set
						if(WriteMethod.SADD == method){
							if(value instanceof String){
								jc.sadd(key, (String)value);
							}
							else if(value instanceof String[]){
								jc.sadd( key, (String[])value );	
							}
						}
						
					}
					else if(ValueType.SORTEDSET == type){
						// sorted set
						if(WriteMethod.ZADD == method){
							jc.zadd(key, (Map<String,Double>)value);
						}
					}
					
					if(expireSeconds > 0){
						// 设置 key的 过期秒数
						jc.expire(key, (int) expireSeconds);
					}
				}
				catch(Exception e){
					logger.error("Can't add to redis!",e);
				}
				finally{
					this.collector.ack(input);
				}
			}
		
		}
		
	}

	@Override
	public void cleanup() {
		// clean resource of redis 
		jc.close();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


	
	

}
