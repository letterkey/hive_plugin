package com.oneapm.hadoop.hive.udaf;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Created by YMY on 16-1-21.
 */
public final class RangeFenBuUDAF extends UDAF {
	public static class RangeFenBuUDAFEvaluator implements UDAFEvaluator {
		private RangeMap<Integer, List<Integer>> rm;
		private int key;

		public void init() {
			this.key = 0;
		}

		public boolean iterate(int value, String sp) {
			String[] splitPoint = sp.split("_");
			Arrays.sort(splitPoint);
			initRangeMap(splitPoint);
			if (this.rm == null)
				return false;
			this.key = value;

			return true;
		}

		public int terminatePartial() {
			return this.key;
		}

		public boolean merge(int k) {
			((List) this.rm.get(Integer.valueOf(k))).add(0,
					Integer.valueOf(((Integer) ((List) this.rm.get(Integer
							.valueOf(k))).get(0)).intValue() + 1));
			return true;
		}

		public String terminate() {
			StringBuffer sb = new StringBuffer();
			Map rangeStringMap = this.rm.asMapOfRanges();
			Set entries = rangeStringMap.entrySet();
			Iterator iterator = entries.iterator();
			while (iterator.hasNext()) {
				Map.Entry next = (Map.Entry) iterator.next();
				sb.append(next.getKey()).append("\t").append(next.getValue());
			}
			return sb.toString();
		}

		public void initRangeMap(String[] splitPoint) {
			if (this.rm == null) {
				this.rm = TreeRangeMap.create();
				Arrays.sort(splitPoint);
				List t = null;
				for (int i = 0; i <= splitPoint.length + 1; i++) {
					t = new ArrayList(1);
					t.add(Integer.valueOf(0));

					if (i == 0) {
						this.rm.put(Range.open(Integer.valueOf(-2147483648),
								Integer.valueOf(splitPoint[i])), t);
					}
					if (i == splitPoint.length - 1) {
						if (i == 0) {
							t = new ArrayList(1);
							t.add(Integer.valueOf(0));
						}
						this.rm.put(Range.closedOpen(
								Integer.valueOf(splitPoint[i]),
								Integer.valueOf(2147483647)), t);
					}
					if ((i > 0) && (i < splitPoint.length - 1))
						this.rm.put(Range.closedOpen(
								Integer.valueOf(splitPoint[i]),
								Integer.valueOf(splitPoint[(i + 1)])), t);
				}
			}
		}
	}
}