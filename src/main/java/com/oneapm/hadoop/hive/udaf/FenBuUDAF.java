package com.oneapm.hadoop.hive.udaf;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Created by YMY on 16-1-21.
 */
public class FenBuUDAF extends UDAF {
	public static double getNum(double num) {
		return new BigDecimal(num).setScale(2, 4).doubleValue();
	}

	public static class FenBuUDAFEvaluator implements UDAFEvaluator {
		private Result partial;

		public void init() {
			this.partial = new Result();
		}

		public boolean iterate(String value, String unitstr, String ignore,
				String addupignore) {
			if (value == null) {
				return true;
			}
			if (this.partial == null) {
				this.partial = new Result();
				this.partial.unit = Long.valueOf(unitstr.trim()).longValue();
				this.partial.ignore = Double.valueOf(ignore.trim())
						.doubleValue();
				this.partial.addupignore = Double.valueOf(addupignore.trim())
						.doubleValue();
			}
			long tk = Double.valueOf(value.trim()).longValue()
					/ this.partial.unit;
			Long tv = (Long) this.partial.map.get(Long.valueOf(tk));
			this.partial.map.put(Long.valueOf(tk), Long.valueOf(tv == null ? 1L
					: (tv = Long.valueOf(tv.longValue() + 1L)).longValue()));
			this.partial.total += 1L;
			return true;
		}

		public Result terminatePartial() {
			return this.partial;
		}

		public boolean merge(Result other) {
			if (other == null) {
				return true;
			}
			for (Map.Entry e : other.map.entrySet()) {
				Long tv = (Long) this.partial.map.get(e.getKey());
				this.partial.map.put(
						(Long) e.getKey(),
						Long.valueOf((tv == null ? 0L : tv.longValue())
								+ ((Long) e.getValue()).longValue()));
			}
			this.partial.total += other.total;
			this.partial.unit = other.unit;
			this.partial.ignore = other.ignore;
			this.partial.addupignore = other.addupignore;
			return true;
		}

		public String terminate() {
			StringBuilder result = new StringBuilder();
			ArrayList list = new ArrayList();
			for (Map.Entry e : this.partial.map.entrySet()) {
				KV kv = new KV();
				kv.K = ((Long) e.getKey()).longValue();
				kv.V = ((Long) e.getValue()).longValue();
				list.add(kv);
			}
			Collections.sort(list, new Comparator<KV>() {
				@Override
				public int compare(KV o1, KV o2) {
					return o1.K < o2.K ? 0 : 1;
				}
			});
			Long start = null;
			Long end = null;
			double tmptotal = 0.0D;
			double adduptotal = 0.0D;

			for (int i = 0; i < list.size(); i++) {
				KV kv = (KV) list.get(i);
				if (start == null) {
					start = kv.K * this.partial.unit;
				}

				tmptotal += kv.V;
				adduptotal += kv.V;
				if ((kv.getRate(this.partial.total) > this.partial.ignore)
						|| (FenBuUDAF.getNum(tmptotal * 100.0D
								/ this.partial.total) > this.partial.addupignore)
						|| (i == list.size() - 1)) {
					end = (kv.K + 1L) * this.partial.unit;

					if ((start == null) || (end == null))
						continue;
					result.append(String.format("%10s", new Object[] { start }))
							.append("~")
							.append(String
									.format("%-10s", new Object[] { end }))
							.append("\t")
							.append(String.format("%-10s",
									new Object[] { Double.valueOf(tmptotal)
											.longValue() }))
							.append("\t")
							.append(FenBuUDAF.getNum(tmptotal * 100.0D
									/ this.partial.total))
							.append("%")
							.append("\t")
							.append(FenBuUDAF.getNum(adduptotal * 100.0D
									/ this.partial.total)).append("%")
							.append("\n");
					start = null;
					end = null;
					tmptotal = 0.0D;
				}
			}
			return result.toString();
		}
	}
}