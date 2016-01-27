package com.oneapm.hadoop.hive.util;

/**
 * Created by YMY on 16-1-21.
 */
public class Pair <L, R>
{
	private L left;
	private R right;

	public L getLeft()
	{
		return this.left;
	}

	public void setLeft(L left) {
		this.left = left;
	}

	public R getRight()
	{
		return this.right;
	}

	public void setRight(R right) {
		this.right = right;
	}

	public Pair(L l, R r) {
		this.left = l;
		this.right = r;
	}
	public Pair() {
	}
	public static <L, R> Pair<L, R> of(L left, R right) {
		return new Pair(left, right);
	}
}