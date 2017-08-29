package com.hapyak.recommendations;


import java.util.*;
import org.json.*;


public interface DataProcessor
{
	boolean isValidDataFile(String fileName);
	boolean getDataFileProcessed(String fileName);
	void setDataFileProcessed(String fileName);
	
	void processHistorySession(String fileName, List<JSONObject> historySessionProjectList);
	
	JSONArray getRecommendationsForCurrentSession(String customerID, List<JSONObject> currentSessionProjectList, int maximumNumberOfRecommendations, boolean includeDetails);
}
