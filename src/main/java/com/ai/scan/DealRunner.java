package com.ai.scan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class DealRunner implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(DealRunner.class);
	private ScanConfig config;
	private IDealService dealService;
	//主线程调用关闭循环，所以加volatile
	private volatile boolean loop = true;

	public DealRunner(ScanConfig config) {
		this.config = config;
		this.dealService = config.getDealService();
	}

	@Override
	public void run() {
		Jedis jedis = this.config.getJedisPool().getResource();
		// jedis监视一个队列
		while (loop) {
			try {
				//断线重连
				if (jedis==null || jedis.isConnected()==false) {
					if(jedis!=null) jedis.close();
					jedis = this.config.getJedisPool().getResource();
				}
				String pushs = jedis.rpop(this.config.getQueueKey());
				if(pushs!=null){
					Object record = dealService.decode(pushs);
					if (record != null) {
						dealService.deal(record);
					}
				}else{
					//如果获取不到数据，则休眠一段时间
					Thread.currentThread().sleep(this.config.getFetchPeriod()*1000);
				}
			} catch (Exception e) {
				LOG.error("处理过程异常"+this.config.getQueueKey(), e);
			}
		}
		
		if(jedis!=null){
			jedis.close();
		}
	}

	public void shutdown() {
		this.loop = false;
	}

}
