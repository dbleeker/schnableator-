package com.hapyak.recommendations;


import java.util.*;
import redis.clients.jedis.*;


public class RedisDatabase implements Database
{
	private Jedis _jedis;

	
	public RedisDatabase(String ipAddressOrDomainName, int portNumber)
    {
		_jedis = new Jedis(ipAddressOrDomainName, portNumber);
    }
	
	
	@Override
	public void destruct()
	{
		_jedis.disconnect();
	}
	
	
	@Override
	public String[] getKeys(String keyWildCard)
	{
		int i, size;
		Iterator<String> iterator;
		Set<String> keySet;
		String[] keys;
		
		keySet = _jedis.keys(keyWildCard);
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
		return (_jedis.get(key));
    }

    
    @Override
    public void write(String key, String value)
    {
		_jedis.set(key, value);
    }
}
