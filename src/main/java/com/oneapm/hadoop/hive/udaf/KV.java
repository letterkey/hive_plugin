package com.oneapm.hadoop.hive.udaf;

import java.math.BigDecimal;

/**
 * Created by YMY on 16-1-21.
 */
public class KV extends Object {
	public long K;
	public long V;

	public double getRate(long total) {
		return new BigDecimal(this.V * 100L
				/ Double.valueOf(total).doubleValue()).setScale(2, 4)
				.doubleValue();
	}
}
