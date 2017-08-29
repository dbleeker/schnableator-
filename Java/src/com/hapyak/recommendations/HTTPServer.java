package com.hapyak.recommendations;


import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;


public class HTTPServer implements HttpHandler
{
    private HttpServer _httpServer;
    private HTTPServerDelegate _httpServerDelegate;
    private int _portNumber;
    private Object _syncObject;


    public HTTPServer(int portNumber, HTTPServerDelegate httpServerDelegate)
    {
        _portNumber = portNumber;
        _httpServerDelegate = httpServerDelegate;
        //
        _syncObject = new Object();
        //
        _httpServer = null;
    }
    
    
    public void destruct()
    {
    	stop();
    }


    public boolean start() throws Exception
    {
        synchronized (_syncObject)
        {
            boolean started;
            HttpsConfigurator httpsConfigurator;

            started = _httpServer != null;
            //
            if (! started)
                try
                {
                    _httpServer = HttpServer.create(new InetSocketAddress(_portNumber), 0);
                    //
                    _httpServer.createContext("/", this);
                    //
                    _httpServer.setExecutor(new ThreadPoolExecutor(4, 16, 30, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100)));
                    //
                    _httpServer.start();
                    //
                    _httpServerDelegate.httpServerDelegateStatus(this, "HTTPServer.start(): started on port " + _portNumber + ".");
                }
                catch (Exception exception)
                {
                	_httpServerDelegate.httpServerDelegateError(this, "HTTPServer.start() error: " + exception.getMessage());
                    stop();
                    throw exception;
                }
            //
            return (! started);
        }
    }


    public boolean stop()
    {
        synchronized (_syncObject)
        {
            boolean started;

            started = _httpServer != null;
            //
            if (started)
            {
                _httpServer.stop(0);
                _httpServer = null;
                //
                _httpServerDelegate.httpServerDelegateStatus(this, "HTTPServer.stop(): stopped.");
            }
            //
            return (started);
        }
    }


    static private Map<String,String> getQueryParameters(HttpExchange httpExchange)
    {
        Map<String,String> map;
        String query;
        String[] keyValue;
        
        map = new HashMap<String,String>();
        //
        query = httpExchange.getRequestURI().getQuery();
        //
        if (query != null)
            for (String queryParameter : query.split("&"))
            {
                keyValue = queryParameter.split("=");
                //
                map.put(keyValue[0], keyValue.length == 1 ? "" : keyValue[1]);
            }
        //
        return (map);
    }


    @Override
    public void handle(HttpExchange httpExchange)
    {
        Map<String,String> queryParameterMap;
        String method, path, query, statusMessage;

        method = httpExchange.getRequestMethod();
        path = httpExchange.getRequestURI().getPath();
        query = httpExchange.getRequestURI().getQuery();
        //
        queryParameterMap = getQueryParameters(httpExchange);
        //
        statusMessage = "Received request";
        statusMessage += "\n\tMethod: " + method;
        statusMessage += "\n\tPath: " + path ;
        statusMessage += "\n\tQuery: " + query;
        //
        _httpServerDelegate.httpServerDelegateStatus(this, statusMessage);
        //
        _httpServerDelegate.httpServerDelegateHandleRequest(this, httpExchange, method, path, queryParameterMap);
    }
}
