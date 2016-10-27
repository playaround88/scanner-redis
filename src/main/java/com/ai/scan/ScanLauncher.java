package com.ai.scan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * 内存扫描表启动程序
 * 
 */
public class ScanLauncher {
	private static final Logger LOG = LoggerFactory.getLogger(ScanLauncher.class);
	private ScanConfig config;
	//线程池
	private ScheduledExecutorService scanThread;
	private ExecutorService pool;
	// deal线程句柄，用于关闭线程
	private List<DealRunner> tasks = new ArrayList<DealRunner>();

	public ScanLauncher(ScanConfig scanConfig) {
		this.config = scanConfig;
	}

	/**
	 * 初始化方法
	 */
	public void init() {
		LOG.debug("启动数据扫描线程...");
		LOG.debug("每次读取条数：" + this.config.getFetchSize());
		LOG.debug("每次读取间隔：" + this.config.getFetchPeriod() + "s");
		// 启动读取数据操作线程
		scanThread = Executors.newSingleThreadScheduledExecutor();
		ScanRunner scanRunner = new ScanRunner(config);
		scanThread.scheduleAtFixedRate(scanRunner, 0, this.config.getFetchPeriod(), TimeUnit.SECONDS);
		LOG.debug("启动数据扫描线程...END");

		// ----------------无聊分割线

		LOG.debug("启动处理线程池...");
		LOG.debug("线程池大小：" + this.config.getPoolSize());
		// 启动一批处理线程
		pool = Executors.newFixedThreadPool(this.config.getPoolSize());
		for (int i = 0; i < this.config.getPoolSize(); i++) {
			DealRunner runner = new DealRunner(i, config);
			tasks.add(runner);
			pool.execute(runner);
		}
		LOG.debug("启动处理线程池...END");
	}

	/**
	 * 销毁方法
	 */
	public void destroy() {
		LOG.debug("read push thread is shutdown...");
		// 先关闭数据库取数据线程
		scanThread.shutdown();

		LOG.debug("read push thread has shutdown");
		Jedis jedis = this.config.getJedisPool().getResource();

		// XXX 要处理完成所有的队列任务再关闭；遍历读取每个队列的长度；
		boolean flag = true;
		while (flag) {
			Map<Integer, Boolean> result = new HashMap<Integer, Boolean>();
			for (int i = 0; i < this.config.getPoolSize(); i++) {
				long len = jedis.llen(this.config.getQueueKey() + ":" + i);
				if (len > 0) {
					result.put(i, true);
				}
			}
			// 任何一个为true，就返回true，否则返回false
			flag = mergeResult(result);
			if (!flag) {
				LOG.debug("all push queue was empty!");
			}
		}

		LOG.debug("shutdown all deal push thread...");
		// 先关闭所有task
		for (int i = 0; i < tasks.size(); i++) {
			tasks.get(i).shutdown();
		}
		LOG.debug("shutdown push deal thread pool...");
		// 关闭线程池
		try {
			pool.shutdown();
			pool.awaitTermination(this.config.getBlockTimeout() + 2, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		jedis.close();

	}

	// 任何一个为true，就返回true，否则返回false
	private boolean mergeResult(Map<Integer, Boolean> result) {
		for (Map.Entry<Integer, Boolean> entry : result.entrySet()) {
			if (entry.getValue()) {
				return true;
			}
		}
		return false;
	}

}
