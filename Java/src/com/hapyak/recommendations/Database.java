package com.hapyak.recommendations;


public interface Database
{
	void destruct();
	String[] getKeys(String keyWildCard);
	String read(String key);
	void write(String key, String value);
}
