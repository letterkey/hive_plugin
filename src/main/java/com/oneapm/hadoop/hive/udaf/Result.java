package com.oneapm.hadoop.hive.udaf;

import java.util.HashMap;

/**
 * Created by YMY on 16-1-21.
 */
class Result{
	public HashMap<Long, Long> map = new HashMap();
	public long total;
	public long unit;
	public double ignore;
	public double addupignore;
}