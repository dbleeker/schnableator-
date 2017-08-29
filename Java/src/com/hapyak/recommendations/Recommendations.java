package com.hapyak.recommendations;


import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.util.*;
import org.json.*;


// kds - need configuration file for all the constants defined just below.
// kds - need to be able to handle data files completely or partially processed already.
// kds - add logic for number of hops from original node (project) and externalize the number-of-hops parameter.
public class Recommendations implements HTTPServerDelegate
{
	static private final boolean VERBOSE = true;
	
	static private final int CHECK_DIRECTORY_CONTENTS_NUMBER_OF_MILLI_SECONDS = 60000;
	
	static private final String DATA_FILE_PATH = "/temp/hapyak";
	
	static private final String REDIS_IP_ADDRESS_OR_DOMAIN_NAME = "localhost";
	static private final int REDIS_PORT_NUMBER = 6379;
	
	static private final int HTTP_SERVER_PORT_NUMBER = 8080;
	
	static private final int DEFAULT_MAXIMUM_NUMBER_OF_RECOMMENDATIONS = 20;
	static private final boolean DEFAULT_INCLUDE_DETAILS = false;
	
	private Database _database;
	private DataProcessor _dataProcessor;
	private HTTPServer _httpServer;
	
	
	public static void main (String[] commandLineArguments) throws Exception
    {
		Recommendations recommendations;
		
        if (commandLineArguments.length != 0)
        	usage();
        //
        recommendations = new Recommendations();
        //
        for (; ; )
        {
        	recommendations.checkDirectoryContents(VERBOSE);
        	//
            Thread.sleep(CHECK_DIRECTORY_CONTENTS_NUMBER_OF_MILLI_SECONDS);
        }
    }


    static private void usage() throws Exception
    {
        System.out.println("Usage: com.hapyak.recommendations.Recommendations");
        //
        throw new Exception();
    }
    
    
    public Recommendations() throws Exception
    {
    	_database = new RedisDatabase(REDIS_IP_ADDRESS_OR_DOMAIN_NAME, REDIS_PORT_NUMBER);
    	//
    	_dataProcessor = new GraphDataProcessor(_database);
    	//
    	_httpServer = new HTTPServer(HTTP_SERVER_PORT_NUMBER, this);
    	//
    	_httpServer.start();
    }
    
    
    public void destruct()
    {
    	_httpServer.destruct();
    	//
    	_database.destruct();
    }

    
	public void checkDirectoryContents(boolean verbose)
    {
    	String[] fileNames;
    	
    	try
    	{
    		fileNames = getDirectoryContents(DATA_FILE_PATH);
    	}
    	catch (Exception exception)
    	{
    		System.out.println("checkDirectoryContents() error: " + exception.getMessage());
    		return;
    	}
    	//
    	for (String fileName : fileNames)
    		if (_dataProcessor.isValidDataFile(fileName))
	    	{
    			if (! _dataProcessor.getDataFileProcessed(fileName))
    			{
    				System.out.println();
		    		System.out.println("Processing " + fileName + "...");
		    		//
		    		processHistoryDataFile(fileName, verbose);
		    		//
		    		_dataProcessor.setDataFileProcessed(fileName);
		    		//
		    		System.out.println("...Done");
    			}
	    	}
    }
    
    
    private void processHistoryDataFile(String fileName, boolean verbose)
    {
    	BufferedReader bufferedReader;
    	int i, lineIndex;
    	int numberOfKeys = 0;
    	JSONObject jsonObject;
    	List<JSONObject> sessionProjectList = null;
    	List<String> lineParts, firstLineParts;
    	String fileSpec, previousSession, line, session;
    	
		fileSpec = DATA_FILE_PATH + File.separator + fileName;
		//
    	bufferedReader = null;
    	//
    	try
    	{
	    	bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(fileSpec)));
	    	//
	    	firstLineParts = null;
	    	//
	    	previousSession = null;
	    	//
	    	for (lineIndex = 0; ; lineIndex ++)
	    	{
	    		line = bufferedReader.readLine();
	    		//
	    		if (line == null)
	    			break;
	    		//
	    		if (verbose)
		    		if (lineIndex % 100000 == 0)
		    			System.out.println("Processed " + lineIndex + " lines...");
	    		//
	    		lineParts = splitCSVLine(line);
	    		//
	    		if (firstLineParts == null)
	    		{
	    			firstLineParts = lineParts;
	    			//
	    			if (findStringInList(firstLineParts, "session") == -1)
	    				System.out.println("processDataFile() error for " + fileSpec + ": 'session' field not found.");
	    			//
	    			numberOfKeys = firstLineParts.size();
	    		}
	    		else
	    		{
	    			if (lineParts.size() != numberOfKeys)
	    				System.out.println("processDataFile() error for " + fileSpec + ": inconsistent number of fields.");
	    			//
	    			jsonObject = new JSONObject();
	    			//
	    			for (i = 0; i < numberOfKeys; i ++)
	    				jsonObject.put(firstLineParts.get(i), lineParts.get(i));
	    			//
	    			session = jsonObject.getString("session");
	    			//
	    			if (previousSession == null)
	    				sessionProjectList = new ArrayList<JSONObject>();
	    			else
	    				if (! session.equals(previousSession))
		    			{
	    			    	_dataProcessor.processHistorySession(fileName, sessionProjectList);
		    				//
		    				sessionProjectList = new ArrayList<JSONObject>();
		    			}
	    			//
	    			sessionProjectList.add(jsonObject);
	    			//
	    			previousSession = session;
	    		}
	    	}
	    	//
	    	if (previousSession != null)
	    		_dataProcessor.processHistorySession(fileName, sessionProjectList);
    	}
    	catch (Exception exception)
    	{
    		System.out.println("processDataFile() error for " + fileSpec + ": " + exception.getMessage());
    	}
    	finally
    	{
    		if (bufferedReader != null)
    			try
    			{
        			bufferedReader.close();
    			}
    			catch (Exception zz)
    			{
    			}
    	}
    }
    
    
    static private List<String> splitCSVLine(String csvLine)
    {
    	boolean doubleQuote;
    	int csvLineLength, offset, startIndex, endIndexPlusOne;
    	List<String> csvLineParts;
    	String csvLinePart;
    	
    	csvLineParts = new ArrayList<String>();
    	//
    	csvLineLength = csvLine.length();
    	//
    	for (offset = 0; offset < csvLineLength; )
    	{
    		doubleQuote = csvLine.charAt(offset) == '"';
    		//
    		if (doubleQuote)
    		{
    			startIndex = offset;
    			//
    		    endIndexPlusOne = findEndDoubleQuoteIndexForCSVLine(csvLine, startIndex);
    		    //
    		    if (endIndexPlusOne == -1)
    		    {
    		    	offset = csvLineLength;
    		    	endIndexPlusOne = csvLineLength;
    		    }
    		    else
    		    	offset = endIndexPlusOne + 1;
    		}
    		else
    		{
    			startIndex = offset;
    			//
    			endIndexPlusOne = csvLine.indexOf(',', startIndex);
    		    //
    		    if (endIndexPlusOne == -1)
    		    {
    		    	offset = csvLineLength;
    		    	endIndexPlusOne = csvLineLength;
    		    }
    		    else
    		    	offset = endIndexPlusOne;
    		}
    		//
    		csvLinePart = csvLine.substring(startIndex, endIndexPlusOne);
    		//
    		if (doubleQuote)
    			csvLinePart = csvLinePart.replace((CharSequence) "\"\"", (CharSequence) "\"");
    		//
    		csvLineParts.add(csvLinePart);
    		//
    		if (offset < csvLineLength)
    			offset ++;
    	}
    	//
    	return (csvLineParts);
    }
    
    
    static private int findEndDoubleQuoteIndexForCSVLine(String csvLine, int startDoubleQuoteIndex)
    {
    	int csvLineLength, offset, doubleQuoteIndex, tempIndex;
    	
    	csvLineLength = csvLine.length();
    	//
    	for (offset = startDoubleQuoteIndex + 1; ; )
    	{
    		doubleQuoteIndex = csvLine.indexOf('"', offset);
    		if (doubleQuoteIndex == -1)
    			return (-1);
    		//
    		tempIndex = doubleQuoteIndex + 1;
    		//
    		if (tempIndex == csvLineLength || csvLine.charAt(tempIndex) != '"')
    			return (doubleQuoteIndex);
    		//
    		offset = tempIndex + 1;
    	}
    }
    
    
    static private int findStringInList(List<String> stringList, String target)
    {
    	int i, numberOfStrings;
    	String string;
    	
    	numberOfStrings = stringList.size();
    	//
    	for (i = 0; i < numberOfStrings; i ++)
    	{
    		string = stringList.get(i);
    		//
    		if (string.equals(target))
    			break;
    	}
    	//
    	return (i == numberOfStrings ? -1 : i);
    }


    static private String[] getDirectoryContents(String filePath) throws Exception
    {
        File[] files;
        int i;
        String[] fileNames;

        files = new File(filePath).listFiles();
        //
        fileNames = new String[files.length];
        //
        for (i = 0; i < files.length; i ++)
            fileNames[i] = files[i].getName();
        //
        return (fileNames);
    }

    
    @Override
    public void httpServerDelegateError(HTTPServer httpServer, String errorMessage)
    {
    	System.out.println(errorMessage);
    }

    
    @Override
	public void httpServerDelegateStatus(HTTPServer httpServer, String statusMessage)
    {
    	System.out.println(statusMessage);
    }

    
    @Override
	public void httpServerDelegateHandleRequest(HTTPServer httpServer, HttpExchange httpExchange, String method, String path, Map<String,String> queryParameterMap)
    {
    	boolean includeDetails;
        Headers responseHeaders;
    	int responseCode, maximumNumberOfRecommendations;
    	JSONArray jsonArray;
    	JSONObject jsonObject;
    	List<JSONObject> currentSessionProjectList;
        OutputStream outputStream;
    	String a, customerID, errorMessage;
    	String jsonText = null;
    	String[] projectIDs;
    	
    	try
    	{
	    	if (path.equals("/getRecommendations"))
	    	{
	    		if (! method.equals("GET"))
	    			throw new Exception();
	    		//
	        	customerID = queryParameterMap.get("customerID");
	        	if (customerID == null)
	    			throw new Exception();
	        	customerID = customerID.trim();
	        	if (customerID.length() == 0)
	    			throw new Exception();
	        	//
	        	a = queryParameterMap.get("projectIDs");
	        	if (a == null)
	    			throw new Exception();
	        	a = a.trim();
	        	if (a.length() == 0)
	        		throw new Exception();
	        	projectIDs = a.split("\\,");
	        	//
	        	a = queryParameterMap.get("maximumNumberOfRecommendations");
	        	if (a == null)
	        		maximumNumberOfRecommendations = DEFAULT_MAXIMUM_NUMBER_OF_RECOMMENDATIONS;
	        	else
	        	{
	        		a = a.trim();
	        		if (a.length() == 0)
	        			throw new Exception();
	        		maximumNumberOfRecommendations = Integer.parseInt(a);
	        		if (maximumNumberOfRecommendations < 1)
	        			throw new Exception();
	        	}
	        	//
	        	a = queryParameterMap.get("includeDetails");
	        	if (a == null)
	        		includeDetails = DEFAULT_INCLUDE_DETAILS;
	        	else
	        	{
	        		a = a.trim();
	        		if (a.length() == 0)
	        			throw new Exception();
	        		if (! a.equals("false") && ! a.equals("true"))
	        			throw new Exception();
	        		includeDetails = a.equals("true");
	        	}
	        	//
	        	currentSessionProjectList = new ArrayList<JSONObject>();
	        	//
	        	for (String projectID : projectIDs)
	        	{
	        		jsonObject = new JSONObject();
	        		//
	        		jsonObject.put("project_id", projectID);
	        		//
	        		currentSessionProjectList.add(jsonObject);
	        	}
	        	//
	        	jsonArray = _dataProcessor.getRecommendationsForCurrentSession(customerID, currentSessionProjectList, maximumNumberOfRecommendations, includeDetails);
	        	//
	        	jsonText = jsonArray.toString();
	        	//
	        	responseCode = HttpURLConnection.HTTP_OK;
	    	}
	    	else
	    		throw new Exception();
    	}
    	catch (Exception exception)
    	{
    		errorMessage = exception.getMessage();
    		if (errorMessage != null)
    			httpServerDelegateError(httpServer, errorMessage);
    		//
	    	responseCode = HttpURLConnection.HTTP_NOT_FOUND;
    	}
    	//
    	try
    	{
	        if (responseCode == HttpURLConnection.HTTP_OK)
	        {
	            responseHeaders = httpExchange.getResponseHeaders();
	        	//
	            responseHeaders.set("Content-Type", "application/json");
	        }
	        //
	        httpExchange.sendResponseHeaders(responseCode, 0);
	        //
	        outputStream = httpExchange.getResponseBody();
	        //
	        if (responseCode == HttpURLConnection.HTTP_OK)
	            outputStream.write(jsonText.getBytes());
	        else
	            outputStream.write("Not Found".getBytes());
	        //
	        outputStream.close();
    	}
    	catch (Exception exception)
    	{
			httpServerDelegateError(httpServer, exception.getMessage());
    	}
	}
}
