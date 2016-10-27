package com.ai.scan;

public interface IDealService<T> {
	/**
	 * 处理记录
	 * @param record
	 */
	void deal(T record);
	/**
	 * 反序列化
	 * @param string
	 * @return
	 */
	T decode(String string);

}
