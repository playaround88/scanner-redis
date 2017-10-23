package com.ai.scan;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import redis.clients.jedis.Jedis;

public class ScanThread extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(ScanThread.class);
	private ScanConfig config;
	private IScanService service;
	private Gson gson = new Gson();
	//主线程调用关闭循环，所以加volatile
	private volatile boolean loop=true;

	public ScanThread(ScanConfig scanConfig) {
		this.config = scanConfig;
		this.service = scanConfig.getScanService();
	}

	@Override
	public void run() {
		super.run();
		LOG.info("启动扫描线程"+this.config.getQueueKey());
		Jedis jedis=this.config.getJedisPool().getResource();
		while(loop){
			try{
				//断线重连
				if(jedis==null || jedis.isConnected()==false){
					if(jedis!=null) jedis.close();
					jedis=this.config.getJedisPool().getResource();
				}
				LOG.debug("扫描数据记录开始");
				List records = service.scan(Integer.valueOf(this.config.getFetchSize()));
				for (Object record : records) {
					// 更新状态
					int row = service.updateStatus(record);
					if (row == 1) {
						// 直接推动到redis队列
						jedis.lpush(this.config.getQueueKey(), gson.toJson(record));
					}
				}
				//如果扫描不到数据，或者扫描的数据小于fetchSize，则休眠一段时间
				if(records==null || records.size()<this.config.getFetchSize()){
					Thread.currentThread().sleep(this.config.getFetchPeriod()*1000);
				}
			}catch (Exception e) {
				LOG.error("扫描数据异常"+this.config.getQueueKey(), e);
			}
		}
		//关闭jedis链接
		if(jedis!=null){
			jedis.close();
		}
	}

	public void shutdown(){
		this.loop=false;
	}
}
