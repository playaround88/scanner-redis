package com.ai.scan;

import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.google.gson.Gson;

public class ScanRunner implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(ScanRunner.class);
	private ScanConfig config;
	private IScanService service;
	private Random random = new Random();
	private Gson gson = new Gson();

	public ScanRunner(ScanConfig scanConfig) {
		this.config = scanConfig;
		this.service = scanConfig.getScanService();
	}

	@Override
	public void run() {
		try {
			Jedis jedis = this.config.getJedisPool().getResource();
			LOG.debug("扫描表记录开始");
			List records = service.scan(Integer.valueOf(this.config.getFetchSize()));
			LOG.debug("取到" + records.size() + "条记录");
			for (Object record : records) {
				// 更新状态
				int row = service.updateStatus(record);
				if (row == 1) {
					// 简单随机分发
					int index = random.nextInt(this.config.getPoolSize());
					jedis.lpush(this.config.getQueueKey() + ":" + index, gson.toJson(record));
				}
			}

			jedis.close();
		} catch (Exception e) {
			LOG.error("扫描表异常：", e);
		}
	}

}
