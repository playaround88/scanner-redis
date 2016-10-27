package com.ai.scan;

import java.util.List;

public interface IScanService<T> {
	/**
	 * 扫描表，取出待处理的N条记录
	 * @param valueOf
	 * @return
	 */
	List<T> scan(Integer fetchSize);
	/**
	 * 更新记录状态，必须确保返回受影响的记录行数为1
	 * @param record
	 * @return
	 */
	int updateStatus(T record);

}
