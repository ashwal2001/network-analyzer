package com.calsoftlabs.hadoop.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.calsoftlabs.hadoop.bo.KeyValue;

public class StringUtils {

	public static boolean startsWithDigit(String s) {
		if (s == null || s.length() == 0)
			return false;

		return Character.isDigit(s.charAt(0));
	}

	public static boolean startsWithLetter(String s) {
		if (s == null || s.length() == 0)
			return false;

		return Character.isLetter(s.charAt(0));
	}

	public static KeyValue convertToken(String token) {
		KeyValue keyValue = null;
		if (token == null || token.length() == 0) {
			keyValue = new KeyValue(null, null);
			return keyValue;
		}

		if (token.contains("=")) {

			Pattern pattern = Pattern.compile("= *");
			Matcher matcher = pattern.matcher(token);
			if (matcher.find()) {
				keyValue = new KeyValue(token.substring(0, matcher.start()),
						token.substring(matcher.end()));
			}
		}
		return keyValue;
	}
	
	public static void main(String[] args) {
		
		KeyValue keyValue = StringUtils.convertToken("src=192.168.18.50");
		System.out.println(keyValue.getKey()+":"+keyValue.getValue());
	}

}