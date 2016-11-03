package com.ai.scan;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class DealRunner implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(DealRunner.class);
	private int index = 0;
	private ScanConfig config;
	private IDealService dealService;
	private volatile boolean startFlag = true;

	public DealRunner(int i, ScanConfig config) {
		this.index = i;
		this.config = config;
		this.dealService = config.getDealService();
	}

	@Override
	public void run() {
		Jedis jedis = null;
		// jedis监视一个队列
		while (startFlag) {
			try {
				if (jedis == null) {
					jedis = this.config.getJedisPool().getResource();
				}
				List<String> pushs = jedis.brpop(this.config.getBlockTimeout(), this.config.getQueueKey() + ":" + this.index);
				if (pushs != null) {
					LOG.debug("线程" + pushs.get(0) + "处理推送消息:" + pushs.get(1));
					Object record = dealService.decode(pushs.get(1));
					dealService.deal(record);
				}
			} catch (Exception e) {
				LOG.error("处理过程异常：", e);
			}
		}
	}

	public void shutdown() {
		this.startFlag = false;
	}

}
