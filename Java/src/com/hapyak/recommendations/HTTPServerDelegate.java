package com.hapyak.recommendations;


import com.sun.net.httpserver.*;
import java.util.*;


public interface HTTPServerDelegate
{
	public void httpServerDelegateError(HTTPServer httpServer, String errorMessage);
	public void httpServerDelegateStatus(HTTPServer httpServer, String statusMessage);
	public void httpServerDelegateHandleRequest(HTTPServer httpServer, HttpExchange httpExchange, String method, String path, Map<String,String> queryParameterMap);
}
