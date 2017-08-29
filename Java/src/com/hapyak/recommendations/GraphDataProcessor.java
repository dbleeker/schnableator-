package com.hapyak.recommendations;


import java.util.*;
import org.json.*;


public class GraphDataProcessor implements DataProcessor
{
	private Database _database;
	
	
	public GraphDataProcessor(Database database)
	{
		_database = database;
	}
	
	
	@Override
	public boolean isValidDataFile(String fileName)
	{
		return (fileName.endsWith(".csv") && fileName.indexOf('-') != -1);
	}
	

	@Override
	public boolean getDataFileProcessed(String fileName)
	{
		return (_database.read(formFileKey(fileName)) != null);
	}

	
	@Override
	public void setDataFileProcessed(String fileName)
	{
		_database.write(formFileKey(fileName), "");
	}
	
	
	@Override
	public void processHistorySession(String fileName, List<JSONObject> historySessionProjectList)
	{
		double fractionWatched, rating, ratingSum;
		double[] fractionsWatched;
		int i, j, size, numberOfPlays;
    	JSONObject jsonObject;
    	String customerID, projectID, title, fractionWatchedText, projectKey, jsonText, linkKey;
    	String[] projectIDs;
    	
    	customerID = getCustomerIDFromDataFile(fileName);
    	//
    	size = historySessionProjectList.size();
    	//
    	projectIDs = new String[size];
    	fractionsWatched = new double[size];
    	//
    	for (i = 0; i < size; i ++)
    	{
    		jsonObject = historySessionProjectList.get(i);
	    	//
    		projectID = jsonObject.getString("project_id");
    		if (projectID == null)
				System.out.println("'project_id' field not found.");
    		projectID = encodeCustomerIDAndProjectID(customerID, projectID);
	    	//
    		title = jsonObject.getString("title");
    		if (title == null)
				System.out.println("'title' field not found.");
	    	//
    		fractionWatchedText = jsonObject.getString("percent_watched");
    		if (fractionWatchedText == null)
				System.out.println("'percent_watched' field not found.");
    		fractionWatched = Double.parseDouble(fractionWatchedText);
    		//
    		projectIDs[i] = projectID;
    		fractionsWatched[i] = fractionWatched;
    		//
    		projectKey = formProjectKey(projectID);
    		//
    		jsonText = _database.read(projectKey);
    		//
    		if (jsonText == null)
    		{
    			jsonObject = new JSONObject();
    			//
        		jsonObject.put("projectID", projectID);
        		jsonObject.put("title", title);
        		//
        		numberOfPlays = 0;
        		rating = 0;
    		}
    		else
    		{
    			jsonObject = new JSONObject(jsonText);
    			//
    			numberOfPlays = jsonObject.getInt("numberOfPlays");
    			rating = jsonObject.getDouble("rating");
    		}
    		//
    		ratingSum = rating * numberOfPlays;
    		//
    		ratingSum += fractionWatched;
    		//
    		numberOfPlays ++;
    		//
    		rating = ratingSum / numberOfPlays;
    		//
    		jsonObject.put("numberOfPlays", numberOfPlays);
    		jsonObject.put("rating", rating);
    		//
    		_database.write(projectKey, jsonObject.toString());
    	}
    	//
    	for (i = 0; i < size; i ++)
    		for (j = 0; j < size; j ++)
    			if (j != i && ! projectIDs[j].equals(projectIDs[i]))
    			{
    				linkKey = formLinkKey(projectIDs[i], projectIDs[j]);
    				//
    				jsonText = _database.read(linkKey);
    				//
    				if (jsonText == null)
    				{
    					jsonObject = new JSONObject();
    					//
    					jsonObject.put("fromProjectID", projectIDs[i]);
    					jsonObject.put("toProjectID", projectIDs[j]);
    					//
    					numberOfPlays = 0;
    					rating = 0;
    				}
    				else
    				{
    					jsonObject = new JSONObject(jsonText);
    					//
    					numberOfPlays = jsonObject.getInt("numberOfPlays");
    					rating = jsonObject.getDouble("rating");
    				}
    	    		//
    	    		ratingSum = rating * numberOfPlays;
    	    		//
    	    		ratingSum += fractionsWatched[j];
    	    		//
    	    		numberOfPlays ++;
    	    		//
    	    		rating = ratingSum / numberOfPlays;
    	    		//
    	    		jsonObject.put("numberOfPlays", numberOfPlays);
    	    		jsonObject.put("rating", rating);
    				//
    				_database.write(linkKey, jsonObject.toString());
    			}
	}
	
	
	@Override
	public JSONArray getRecommendationsForCurrentSession(String customerID, List<JSONObject> currentSessionProjectList, int maximumNumberOfRecommendations, boolean includeDetails)
	{
		double fromProjectRating, linkRating, toProjectRating;
		int i, j, size, fromProjectNumberOfPlays, linkNumberOfPlays, toProjectNumberOfPlays;
		JSONArray recommendationJSONArray;
		JSONObject fromProjectJSONObject, linkJSONObject, toProjectJSONObject, recommendationJSONObject;
		List<Recommendation> recommendationList, sortedRecommendationList;
		Recommendation recommendation, tempRecommendation;
		String fromProjectID, fromProjectTitle, jsonText, toProjectID, toProjectTitle;
		String[] projectIDs, linkKeys, customerIDAndProjectID;
		
    	//
    	//  create recommendations by looping on each from project and then looping on
    	//  each of its links.
    	//
		recommendationList = new ArrayList<Recommendation>();
    	//
    	size = currentSessionProjectList.size();
		//
		projectIDs = new String[size];
    	//
    	for (i = 0; i < size; i ++)
    	{
    		fromProjectJSONObject = currentSessionProjectList.get(i);
	    	//
    		fromProjectID = fromProjectJSONObject.getString("project_id");
    		fromProjectID = encodeCustomerIDAndProjectID(customerID, fromProjectID);
    		//
    		projectIDs[i] = fromProjectID;
    		//
    		jsonText = _database.read(formProjectKey(fromProjectID));
    		//
    		if (jsonText != null)
    		{
    			fromProjectJSONObject = new JSONObject(jsonText);
	    		//
    			fromProjectTitle = fromProjectJSONObject.getString("title");
    			fromProjectNumberOfPlays = fromProjectJSONObject.getInt("numberOfPlays");
    			fromProjectRating = fromProjectJSONObject.getDouble("rating");
    			//
	    		linkKeys = _database.getKeys(formLinkKey(fromProjectID, null));
	    		//
	    		for (j = 0; j < linkKeys.length; j ++)
	    		{
	        		jsonText = _database.read(linkKeys[j]);
	        		//
	        		if (jsonText != null)
	        		{
	        			linkJSONObject = new JSONObject(jsonText);
	        			//
	        			toProjectID = linkJSONObject.getString("toProjectID");
	        			linkNumberOfPlays = linkJSONObject.getInt("numberOfPlays");
	        			linkRating = linkJSONObject.getDouble("rating");
	        			//
	        			jsonText = _database.read(formProjectKey(toProjectID));
	        			//
	        			if (jsonText != null)
	        			{
	        				toProjectJSONObject = new JSONObject(jsonText);
	        				//
	            			toProjectTitle = toProjectJSONObject.getString("title");
	        				toProjectNumberOfPlays = toProjectJSONObject.getInt("numberOfPlays");
	        				toProjectRating = toProjectJSONObject.getDouble("rating");
	        				//
	        				recommendation = new Recommendation(fromProjectID, fromProjectTitle, fromProjectNumberOfPlays, fromProjectRating,
	        								linkNumberOfPlays, linkRating,
	        								toProjectID, toProjectTitle, toProjectNumberOfPlays, toProjectRating);
	        				//
	        				recommendationList.add(recommendation);
	        			}
	        		}
	    		}
    		}
    	}
    	//
    	//  remove any recommendations with a to-project id that is in the original list of project
    	//  ids.  (we don't want to recommend any videos that the user has already viewed.)
    	//
    	size = recommendationList.size();
    	//
    	for (i = 0; i < size; )
    	{
    		recommendation = recommendationList.get(i);
    		//
    		for (j = 0; j < projectIDs.length; j ++)
    			if (projectIDs[j].equals(recommendation._toProjectID))
    				break;
    		//
    		if (j == projectIDs.length)
    			i ++;
    		else
    		{
    			recommendationList.remove(i);
    			size --;
    		}
    	}
    	//
    	//  sort the recommendations.
    	//
    	sortedRecommendationList = new ArrayList<Recommendation>();
    	//
    	size = recommendationList.size();
    	//
    	for (i = 0; i < size; i ++)
    	{
    		recommendation = recommendationList.get(i);
    		//
    		for (j = 0; j < i; j ++)
    		{
    			tempRecommendation = sortedRecommendationList.get(j);
    			//
    			if (compareRecommendations(recommendation, tempRecommendation) < 1)
    				break;
    		}
    		//
    		sortedRecommendationList.add(j, recommendation);
    	}
    	//
    	recommendationList = sortedRecommendationList;
    	//
    	//  remove any duplicate recommendations (with the same to-project id).
    	//
    	size = recommendationList.size();
    	//
    	for (i = 0; i < size; i ++)
    	{
    		recommendation = recommendationList.get(i);
    		//
    		for (j = i + 1; j < size; )
    		{
    			tempRecommendation = recommendationList.get(j);
    			//
    			if (tempRecommendation._toProjectID.equals(recommendation._toProjectID))
    			{
    				recommendationList.remove(j);
    				//
    				size --;
    			}
    			else
    				j ++;
    		}
    	}
    	//
    	//  remove extra recommendations (more than the maximum number of recommendations).
    	//
    	for (size = recommendationList.size(); size > maximumNumberOfRecommendations; size --)
    		recommendationList.remove(size - 1);
    	//
    	//  convert the recommendation list to a json array.
    	//
    	recommendationJSONArray = new JSONArray();
    	//
    	for (i = 0; i < size; i ++)
    	{
    		recommendation = recommendationList.get(i);
    		//
    		recommendationJSONObject = new JSONObject();
    		//
    		customerIDAndProjectID = decodeCustomerIDAndProjectID(recommendation._toProjectID);
    		recommendationJSONObject.put("projectID", customerIDAndProjectID[1]);
    		recommendationJSONObject.put("title", recommendation._toProjectTitle);
    		//
    		if (includeDetails)
    		{
	    		customerIDAndProjectID = decodeCustomerIDAndProjectID(recommendation._fromProjectID);
	    		recommendationJSONObject.put("fromProjectID", customerIDAndProjectID[1]);
	    		recommendationJSONObject.put("fromProjectTitle", recommendation._fromProjectTitle);
	    		recommendationJSONObject.put("fromProjectNumberOfPlays", recommendation._fromProjectNumberOfPlays);
	    		recommendationJSONObject.put("fromProjectRating", recommendation._fromProjectRating);
	    		//
	    		recommendationJSONObject.put("linkNumberOfPlays", recommendation._linkNumberOfPlays);
	    		recommendationJSONObject.put("linkRating", recommendation._linkRating);
	    		//
	    		customerIDAndProjectID = decodeCustomerIDAndProjectID(recommendation._toProjectID);
	    		recommendationJSONObject.put("toProjectID", customerIDAndProjectID[1]);
	    		recommendationJSONObject.put("toProjectTitle", recommendation._toProjectTitle);
	    		recommendationJSONObject.put("toProjectNumberOfPlays", recommendation._toProjectNumberOfPlays);
	    		recommendationJSONObject.put("toProjectRating", recommendation._toProjectRating);
    		}
    		//
    		recommendationJSONArray.put(recommendationJSONObject);
    	}
    	//
    	//  done.
    	//
		return (recommendationJSONArray);
	}
	
	
	static private String formFileKey(String fileName)
	{
		return ("file:" + fileName);
	}
	
	
	static private String formProjectKey(String projectID)
	{
		return ("project:" + projectID);
	}
	
	
	static private String formLinkKey(String fromProjectID, String toProjectID)
	{
		return ("link:" + (fromProjectID == null ? "*" : fromProjectID) + ":" + (toProjectID == null ? "*" : toProjectID));
	}
	
	
	static private String getCustomerIDFromDataFile(String fileName)
	{
		return (fileName.substring(0, fileName.indexOf('-')));
	}
	
	
	static private String encodeCustomerIDAndProjectID(String customerID, String projectID)
	{
		return (customerID + "-" + projectID);
	}
	
	
	static private String[] decodeCustomerIDAndProjectID(String encodedProjectID)
	{
		int index;
		String[] customerIDAndProjectID;
		
		customerIDAndProjectID = new String[2];
		//
		index = encodedProjectID.indexOf('-');
		//
		customerIDAndProjectID[0] = encodedProjectID.substring(0, index);
		customerIDAndProjectID[1] = encodedProjectID.substring(index + 1);
		//
		return (customerIDAndProjectID);
	}
	
	
	static private int compareRecommendations(Recommendation recommendation0, Recommendation recommendation1)
	{
		if (recommendation0._linkNumberOfPlays > recommendation1._linkNumberOfPlays)
			return (-1);
		else if (recommendation0._linkNumberOfPlays < recommendation1._linkNumberOfPlays)
			return (1);
		else
			if (recommendation0._linkRating > recommendation1._linkRating)
				return (-1);
			else if (recommendation0._linkRating < recommendation1._linkRating)
				return (1);
			else
				if (recommendation0._toProjectRating > recommendation1._toProjectRating)
					return (-1);
				else if (recommendation0._toProjectRating < recommendation1._toProjectRating)
					return (1);
				else
					return (0);
	}
	
	
	static private class Recommendation
	{
		public double _fromProjectRating, _linkRating, _toProjectRating;
		public int _fromProjectNumberOfPlays, _linkNumberOfPlays, _toProjectNumberOfPlays;
		public String _fromProjectID, _fromProjectTitle, _toProjectID, _toProjectTitle;
		
		
		public Recommendation(String fromProjectID, String fromProjectTitle, int fromProjectNumberOfPlays, double fromProjectRating,
						int linkNumberOfPlays, double linkRating,
						String toProjectID, String toProjectTitle, int toProjectNumberOfPlays, double toProjectRating)
		{
			_fromProjectID = fromProjectID;
			_fromProjectTitle = fromProjectTitle;
			_fromProjectNumberOfPlays = fromProjectNumberOfPlays;
			_fromProjectRating = fromProjectRating;
			//
			_linkNumberOfPlays = linkNumberOfPlays;
			_linkRating = linkRating;
			//
			_toProjectID = toProjectID;
			_toProjectTitle = toProjectTitle;
			_toProjectNumberOfPlays = toProjectNumberOfPlays;
			_toProjectRating = toProjectRating;
		}
	}
}
