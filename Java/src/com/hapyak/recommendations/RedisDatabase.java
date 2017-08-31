package com.hapyak.recommendations;


import java.util.*;
import redis.clients.jedis.*;


public class RedisDatabase implements Database
{
	private Jedis _jedis;
	private Object _syncObject;

	
	public RedisDatabase(String ipAddressOrDomainName, int portNumber)
	{
		_syncObject = new Object();
		//
		_jedis = new Jedis(ipAddressOrDomainName, portNumber);
	}
	
	
	@Override
	public void destruct()
	{
		synchronized (_syncObject)
		{
			if (_jedis != null)
			{
				_jedis.disconnect();
				_jedis = null;
			}
		}
	}
	
	
	@Override
	public String[] getKeys(String keyWildCard)
	{
		int i, size;
		Iterator<String> iterator;
		Set<String> keySet;
		String[] keys;
		
		synchronized (_syncObject)
		{
			keySet = _jedis.keys(keyWildCard);
		}
		//
		size = keySet.size();
		//
		keys = new String[size];
		//
		iterator = keySet.iterator();
		//
		for (i = 0; i < size; i ++)
			keys[i] = iterator.next();
		//
		return (keys);
	}


	@Override
	public String read(String key)
	{
		synchronized (_syncObject)
		{
			return (_jedis.get(key));
		}
	}


	@Override
	public void write(String key, String value)
	{
		synchronized (_syncObject)
		{
			_jedis.set(key, value);
		}
	}
}
