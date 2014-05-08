package collectURL;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

import javax.net.ssl.HttpsURLConnection;

import model.UrlSource;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import exceptions.TwitterException;

public class Twitter extends TimerTask implements Runnable{
    static private final Logger LOGGER = Logger.getLogger(Twitter.class);
    private final Ehcache cache; //Create a cache to cache URL discovered.
    private final String authenticationEndPoint;
    private final String deauthenticationEndPoint;
	private final Integer count; //Number of results per page that should be returned by the search API.
	private final String resultType; // Twitter Search result type: either mixed or recent.
	private final Map<String, SearchState> searchTermsStateMap;
	private final String[] searchTerms;
	private int currentSearchIndex; //Indexes into the searchTerms array to indicate which search term is
	                                //being currently searched for.
	private final String language;
	private final BlockingQueue<UrlSource> queue;
	private String authenticationToken = null; //Bearer token
	private String bearerTokenCedentials = null;
	private boolean authenticated = false;
	
	public Twitter(BlockingQueue<UrlSource> q) throws TwitterException{
	    CacheManager manager = CacheManager.newInstance("config/ehcache.xml");
	    cache = manager.getCache("madaapTwitter");
	    
		queue = q;
		XMLConfiguration config = getConfiguration();
		this.count = config.getInt("query.count");
		this.language = config.getString("query.lang");
		this.resultType = config.getString("query.resultType");
		
		this.authenticationEndPoint = config.getString("oauth.authEndpoint");
		this.deauthenticationEndPoint = config.getString("oauth.deauthEndpoint");
		
		searchTermsStateMap = new HashMap<String, Twitter.SearchState>();
		try {
		    List<Object> searchTerms = config.getList("query.terms");
		    for (Object searchTerm : searchTerms) {
		        this.searchTermsStateMap.put(URLEncoder.encode((String)searchTerm, "UTF-8"),
		                                     new SearchState());
		    }
		    this.searchTerms = searchTermsStateMap.keySet().toArray(ArrayUtils.EMPTY_STRING_ARRAY);
		    this.currentSearchIndex = 0;

        } catch (UnsupportedEncodingException e) {
            throw new TwitterException(e);
        }
	}
	
	public void authenticate() throws TwitterException {
	    XMLConfiguration config = getConfiguration();
	    String consumerKey = config.getString("oauth.key");
	    String consumerSecret = config.getString("oauth.secret");
	    String encodedKeys = encodeKeys(consumerKey, consumerSecret);
	    this.bearerTokenCedentials = encodedKeys;
	    
	    this.authenticationToken = getBearerToken(authenticationEndPoint, encodedKeys);
	    LOGGER.info("Successfully fetched Twitter bearer token.");
	    authenticated = true;
	}
	
	public boolean isAuthenticated() {
	    return authenticated;
	}
	
	public void deauthenticate() throws TwitterException {
	    if (authenticated == false) {
	        LOGGER.info("Twitter not authenticated. No need for deauthentication");
	        return;
	    }
	    try {
	        
	        URL endpointUrl = new URL(deauthenticationEndPoint);

	        HttpsURLConnection conn = (HttpsURLConnection)endpointUrl.openConnection();
	        conn.setDoInput(true);
	        conn.setDoOutput(true);
	        conn.setRequestMethod("POST");
	        conn.setRequestProperty("Host", "api.twitter.com");
	        conn.setRequestProperty("User-Agent", "Test");
	        conn.setRequestProperty("Authorization", "Basic "+this.bearerTokenCedentials);
	        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
	        conn.setUseCaches(false);
	        
	        String request = "access_token="+this.authenticationToken;
	        postRequest(conn, request);
	        JSONObject response = new JSONObject(readResponse(conn));
	        String accessToken = response.getString("access_token");
            if (accessToken == null || accessToken.equals(this.authenticationToken) == false) {
                throw new TwitterException("Failed to deauthenticate bearer token.");
            }
            LOGGER.info("Successfully deauthenticated Twitter bearer token.");

	    } catch (MalformedURLException e) {
	        throw new TwitterException(e);
	    } catch (IOException e) {
	        throw new TwitterException(e);
	    } catch (JSONException e) {
            throw new TwitterException(e);
        }
	}
	
	private String encodeKeys(String consumerKey, String consumerSecret) throws TwitterException {
	    try {
            String encodedConsumerKey = URLEncoder.encode(consumerKey, "UTF-8");
            String encodedCosnumerSecret = URLEncoder.encode(consumerSecret, "UTF-8");
            String fullKey = encodedConsumerKey+":"+encodedCosnumerSecret;
            byte[] encodedBytes = Base64.encodeBase64(fullKey.getBytes());
            return new String(encodedBytes);
        } catch (UnsupportedEncodingException e) {
            throw new TwitterException(e);
        }
	}
	
	private String getBearerToken(String endpointUrlString, String encodedCredentials) throws TwitterException {
	    String stringResponse = "";
	    try {
            URL endpointUrl = new URL(endpointUrlString);
            HttpsURLConnection conn = (HttpsURLConnection)endpointUrl.openConnection();
            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Host", "api.twitter.com");
            conn.setRequestProperty("User-Agent", "Test");
            conn.setRequestProperty("Authorization", "Basic "+encodedCredentials);
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
            conn.setUseCaches(false);
            
            String request = "grant_type=client_credentials";
            postRequest(conn, request);
            stringResponse = readResponse(conn);
            JSONObject response = new JSONObject(stringResponse);
            
            String tokenType = response.getString("token_type");
            String token = response.getString("access_token");
            if ( tokenType.equals("bearer") && token != null ) {
                return token;
            }
            else {
                throw new TwitterException("Could not get bearer token.");
            }
            
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);            
        } catch (IOException e) {
            LOGGER.error("Could not connect to Twitter end point: "+endpointUrlString);
            throw new TwitterException();
        } catch (JSONException e) {
            LOGGER.error("Failed to parse Json response from: "+stringResponse);
            throw new TwitterException(e);
        }
	}
	
	private void postRequest(HttpsURLConnection connection, String request) throws TwitterException {
	    connection.setRequestProperty("Content-Length", ""+request.length());
	    try {
            OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
            writer.write(request);
            writer.flush();
            writer.close();
            
        } catch (IOException e) {
            LOGGER.error("Could not post request");
            throw new TwitterException(e);
        }
	}
	
	private String readResponse(HttpsURLConnection connection) throws TwitterException {
	    try {
            InputStream inputStream= connection.getInputStream();
            return IOUtils.toString(inputStream);
        } catch (IOException e) {
            LOGGER.error("Could not read response from connection");
            throw new TwitterException(e);
        }

	}
	
	private void collect() throws TwitterException{
		try {
            URL endpointUrl = getTwitterUri().toURL();
            HttpsURLConnection conn = (HttpsURLConnection)endpointUrl.openConnection();
            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Host", "api.twitter.com");
            conn.setRequestProperty("User-Agent", "AsishTest");
            conn.setRequestProperty("Authorization", "Bearer "+this.authenticationToken);
            conn.setUseCaches(false);
            String response = readResponse(conn);
            parseResponse(response);
            
            //Get the next search term
            this.currentSearchIndex = (this.currentSearchIndex + 1) % this.searchTerms.length;
            LOGGER.debug("Current search : "+this.searchTerms[this.currentSearchIndex]);
            
        } catch (JSONException e) {
            LOGGER.error("Error parsing Json. Ignoring response",e);
            throw new TwitterException(e);
        } catch (ClientProtocolException e) {
            throw new TwitterException(e);
        } catch (IOException e) {
            throw new TwitterException(e);
        }
	}
	
    private void parseResponse(String response) throws IOException,
            JSONException {
		JSONArray urls = null;
		
		if (response == null){
		    return;
		}
		
		JSONObject twitterJson = new JSONObject(response);
		JSONArray resultArray = twitterJson.getJSONArray("statuses");
		JSONObject metadata = twitterJson.getJSONObject("search_metadata");
		SearchState searchState = this.searchTermsStateMap.get(this.searchTerms[this.currentSearchIndex]);
		searchState.sinceId = Long.valueOf(metadata.get("max_id_str").toString());
		Long minId = Long.MAX_VALUE;
		searchState.lastCount = metadata.getInt("count");

		for(int i=0;i<resultArray.length();i++){
		    JSONObject currentResult = resultArray.getJSONObject(i); 
		    Long currentId = Long.parseLong(currentResult.get("id_str").toString());
		    if (currentId < minId) {
		        minId = currentId;
		    }
		    urls = currentResult.getJSONObject("entities").getJSONArray("urls");
		    for (int j=0;j<urls.length();j++){
		        if (urls.get(j).toString()!=""){
		            URL currentURL = new URL(urls.getJSONObject(j).get("expanded_url").toString());
		            URL expandedURL = expandURL(currentURL);
		            addToEventQueue(expandedURL); 
		            LOGGER.debug("Twitter URL: "+expandedURL);
		        }
		        else
		            LOGGER.warn("empty url");
		    }
		}
		searchState.maxId = minId - 1;
		LOGGER.debug("Max Id = "+searchState.maxId);
		LOGGER.debug("since_id = "+searchState.sinceId);
		LOGGER.debug("Count = "+searchState.lastCount);
		
    }
    
    private void addToEventQueue(URL expandedURL) {
        try {
            if (cache.get(expandedURL.toString()) == null) {
                LOGGER.debug("URL: "+expandedURL.toString()+" not present in cache. Adding to queue.");
                queue.put(new UrlSource(null, expandedURL, UrlSource.Source.SOCIAL_MEDIA));
                cache.put(new Element(expandedURL.toString(), expandedURL));
            }
            else {
                LOGGER.debug("URL: "+expandedURL.toString()+" present in cache. Skipping.");
            }
        } catch (InterruptedException e) {
            LOGGER.fatal("Failed to add to queue.", e);
            throw new RuntimeException(e);
        }
    }
    
    private URI getTwitterUri() {
        URI uri;
        try {
            URIBuilder builder = new URIBuilder()
                .setScheme("https")
                .setHost("api.twitter.com")
                .setPath("/1.1/search/tweets.json")
                .addParameter("q", this.searchTerms[this.currentSearchIndex])
                .addParameter("count", this.count.toString())
                .addParameter("lang", this.language)
                .addParameter("include_entities", "true")
                .addParameter("result_type", this.resultType)
                .setFragment(null);
            
            SearchState searchState = this.searchTermsStateMap.get(this.searchTerms[this.currentSearchIndex]);
                    
            if (searchState.sinceId > 0 && searchState.lastCount < this.count) {
                builder.addParameter("since_id", searchState.sinceId.toString());
            }
            if (searchState.maxId > 0 && searchState.lastCount >= this.count) {
                builder.addParameter("max_id", searchState.maxId.toString());
            }
            builder.addParameter("Authorization", "Bearer "+URLEncoder.encode(this.authenticationToken, "UTF-8"));
            uri = builder.build();
            
        } catch (URISyntaxException e) {
            LOGGER.fatal("Failed to create Twitter URI.", e);
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return uri;
    }
	
    private XMLConfiguration getConfiguration() {
        XMLConfiguration config;
        try {
            config = new XMLConfiguration("config/twitter.xml");
        } catch (ConfigurationException e) {
            LOGGER.fatal("Not able to configure Twitter.", e);
            throw new RuntimeException(e);
        }
        return config;
    }
	
	/*Expand shortened URL by getting Header field
	 * Visible across package collectURL*/
	URL expandURL(URL url) throws IOException{
		HttpURLConnection connection = null;
		try{
			connection = (HttpURLConnection) url.openConnection();
			connection.setConnectTimeout(20000); //20 second timeout
			connection.setInstanceFollowRedirects(false);
			connection.connect();
			String header = connection.getHeaderField("Location");
			if (header!=null){
				URL expandedURL = new URL(header);
				return expandedURL;
			}
		}
		finally{
			if (connection!=null){
				((HttpURLConnection)connection).disconnect();
			}
		}
		return url;
	}
	
	@Override
	public void run() {
		try {
			collect();
		} catch (Exception e) {
		    try {
                deauthenticate();
            } catch (TwitterException e1) {
                throw new RuntimeException(e);
            }
		    throw new RuntimeException(e);
		}
		
	}
	
	/**
	 * Encapsulates the search state for a particular search query.
	 * @author asish
	 */
	private static class SearchState {
	    private Long sinceId = -1L; //Used to keep track of tweets in the timeline.
	    private Long maxId = -1L;  //Used to keep track of tweets in the timeline.
	    private Integer lastCount = -1; //Number of records returned by last query.
	    
	};
}
