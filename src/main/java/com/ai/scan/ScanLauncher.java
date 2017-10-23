package com.ai.scan;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 内存扫描表启动程序
 * 
 */
public class ScanLauncher {
	private static final Logger LOG = LoggerFactory.getLogger(ScanLauncher.class);
	private ScanConfig config;
	//监听线程
	private ScanThread scanThread;
	//线程池
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
		// 启动读取数据操作线程
		scanThread = new ScanThread(config);
		scanThread.start();
		LOG.debug("启动数据扫描线程...END");

		// ----------------无聊分割线

		LOG.debug("启动处理线程池...");
		LOG.debug("线程池大小：" + this.config.getPoolSize());
		// 启动一批处理线程
		pool = Executors.newFixedThreadPool(this.config.getPoolSize());
		for (int i = 0; i < this.config.getPoolSize(); i++) {
			DealRunner runner = new DealRunner(config);
			tasks.add(runner);
			pool.execute(runner);
		}
		LOG.debug("启动处理线程池...END");
	}

	/**
	 * 销毁方法
	 */
	public void destroy() {
		LOG.debug("scan thread is shutdown...");
		// 先关闭数据库取数据线程
		scanThread.shutdown();
		LOG.debug("scan thread has shutdown");
		
		LOG.debug("shutdown all deal thread...");
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

	}
}
