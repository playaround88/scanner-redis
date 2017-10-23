package com.ai.scan;

import java.io.Serializable;

import redis.clients.jedis.JedisPool;

/**
 * 扫描程序的配置封装;
 * 不支持动态修改参数
 * @author wutb
 *
 */
public class ScanConfig implements Serializable {
	private static final long serialVersionUID = 2140695274817677757L;
	//扫描表，每次取数据的条数
	private int fetchSize = 100;
	//当扫描不到数据，或者得到的数据小于fetchSize时，休眠时间
	private long sleepTime = 10;
	//处理线程的个数
	private int poolSize = 5;
	//处理线程，关闭等待时间
	private int blockTimeout = 10;
	//队列在redis中key的prefix
	private String queueKey="queue:push";
	//
	private IScanService scanService;
	private IDealService dealService;
	//
	private JedisPool jedisPool;

	public ScanConfig(int fetchSize, long sleepTime, int poolSize, int blockTimeout,String queueKey,
			IScanService scanService, IDealService dealService, JedisPool jedisPool) {
		this.fetchSize = fetchSize;
		this.sleepTime = sleepTime;
		this.poolSize = poolSize;
		this.blockTimeout=blockTimeout;
		this.queueKey=queueKey;
		//
		this.scanService = scanService;
		this.dealService = dealService;
		//
		this.jedisPool=jedisPool;
	}

	public int getFetchSize() {
		return fetchSize;
	}

	public long getSleepTime() {
		return sleepTime;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public int getBlockTimeout() {
		return blockTimeout;
	}

	public String getQueueKey() {
		return queueKey;
	}

	public JedisPool getJedisPool() {
		return jedisPool;
	}

	public IScanService getScanService() {
		return scanService;
	}

	public IDealService getDealService() {
		return dealService;
	}
}
