package com.oneapm.hadoop.hive.util;

import java.util.ArrayList;
import java.util.List;
/**
 * Created by YMY on 16-1-21.
 */
public class StringUtils {
	public static final int INDEX_NOT_FOUND = -1;
	public static final String[] EMPTY_STRING_ARRAY = new String[0];

	public static boolean isNotBlank(CharSequence cs) {
		return !isBlank(cs);
	}

	public static boolean isBlank(CharSequence cs)
	{
		int strLen = 0;
		if ((cs == null) || ((strLen = cs.length()) == 0))
			return true;

		for (int i = 0; i < strLen; i++) {
			if (!Character.isWhitespace(cs.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	public static String replace(String text, String searchString, String replacement)
	{
		return replace(text, searchString, replacement, -1);
	}

	public static boolean isEmpty(CharSequence cs) {
		return (cs == null) || (cs.length() == 0);
	}

	public static boolean isNotEmpty(CharSequence cs) {
		return !isEmpty(cs);
	}

	public static String replace(String text, String searchString, String replacement, int max)
	{
		if ((isEmpty(text)) || (isEmpty(searchString)) || (replacement == null) ||
				(max == 0)) {
			return text;
		}
		int start = 0;
		int end = text.indexOf(searchString, start);
		if (end == -1) {
			return text;
		}
		int replLength = searchString.length();
		int increase = replacement.length() - replLength;
		increase = increase < 0 ? 0 : increase;
		increase *= (max > 64 ? 64 : max < 0 ? 16 : max);
		StringBuilder buf = new StringBuilder(text.length() + increase);
		while (end != -1) {
			buf.append(text.substring(start, end)).append(replacement);
			start = end + replLength;
			max--; if (max == 0) {
				break;
			}
			end = text.indexOf(searchString, start);
		}
		buf.append(text.substring(start));
		return buf.toString();
	}

	public static String[] split(String str, String separatorChars) {
		return splitWorker(str, separatorChars, -1, false);
	}

	private static String[] splitWorker(String str, String separatorChars, int max, boolean preserveAllTokens)
	{
		if (str == null) {
			return null;
		}
		int len = str.length();
		if (len == 0) {
			return EMPTY_STRING_ARRAY;
		}
		List list = new ArrayList();
		int sizePlus1 = 1;
		int i = 0; int start = 0;
		boolean match = false;
		boolean lastMatch = false;
		if (separatorChars == null)
		{
			while (i < len)
				if (Character.isWhitespace(str.charAt(i))) {
					if ((match) || (preserveAllTokens)) {
						lastMatch = true;
						if (sizePlus1++ == max) {
							i = len;
							lastMatch = false;
						}
						list.add(str.substring(start, i));
						match = false;
					}
					i++; start = i;
				}
				else {
					lastMatch = false;
					match = true;
					i++;
				}
		} else if (separatorChars.length() == 1)
		{
			char sep = separatorChars.charAt(0);
			while (i < len)
				if (str.charAt(i) == sep) {
					if ((match) || (preserveAllTokens)) {
						lastMatch = true;
						if (sizePlus1++ == max) {
							i = len;
							lastMatch = false;
						}
						list.add(str.substring(start, i));
						match = false;
					}
					i++; start = i;
				}
				else {
					lastMatch = false;
					match = true;
					i++;
				}
		}
		else {
			do
				if (separatorChars.indexOf(str.charAt(i)) >= 0) {
					if ((match) || (preserveAllTokens)) {
						lastMatch = true;
						if (sizePlus1++ == max) {
							i = len;
							lastMatch = false;
						}
						list.add(str.substring(start, i));
						match = false;
					}
					i++; start = i;
				}
				else {
					lastMatch = false;
					match = true;
					i++;
				}
			while (i < len);
		}

		if ((match) || ((preserveAllTokens) && (lastMatch))) {
			list.add(str.substring(start, i));
		}
		return (String[])list.toArray(new String[list.size()]);
	}
}