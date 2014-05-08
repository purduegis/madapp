package extractor;
import exceptions.TwitterException;
import gate.Gate;
import gate.creole.ANNIEConstants;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import model.UrlSource;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import collectURL.ManualFeeder;
import collectURL.Twitter;


public class Madaap {

	/**
	 * Starting point of application
	 * Read Madaap documentation before studying code.
	 * @param args
	 * @throws Exception
	 * 
	 */
	static final int ONE_MILLSEC = 1;
	static final int ONE_SEC = 1000*ONE_MILLSEC;
	static final int ONE_MIN = 60*ONE_SEC;
	static final int ONE_HOUR = 60*ONE_MIN;
	static final int ONE_DAY = 24*ONE_HOUR;
	public static final XMLConfiguration config; 
	private static final Logger LOGGER = Logger.getLogger(Madaap.class);
	
	static {
	    try {
            config = new XMLConfiguration(constructPath("config","madaap.xml"));
        } catch (ConfigurationException e) {
            throw new RuntimeException("Could not read madaap.xml");
        }
	}
	
	
	public static void main(String[] args) throws Exception{
		// Initialize log4j
	    PropertyConfigurator.configure(constructPath("config","log4j.xml"));
		/*Set to your Gate installation home*/
		Gate.setGateHome(new File(config.getString("gate.home")));
		
		Gate.init();
		
		Gate.getCreoleRegister().registerDirectories(new File(Gate.getPluginsHome(), ANNIEConstants.PLUGIN_DIR).toURI().toURL());
		
		/*Set the path to \Plugins\Tools directory*/
		Gate.getCreoleRegister().registerDirectories(
		        new File(constructPath(config.getString("gate.home"),"plugins","Tools"))
		            .toURI()
		            .toURL()); 
		
		/*Declare queue to receive URL from various collectors*/
		BlockingQueue<UrlSource> queue = new LinkedBlockingQueue<UrlSource>();
		
		/*Start extractor, initialization will run the thread*/
		Extractor e = new Extractor(queue);
		
		/*Timer to schedule collector tasks at regular intervals*/
		Timer timer = new Timer();
		
		/*Collect URL from /input/url.txt*/
		TimerTask manualFeederTask = new ManualFeeder(queue);
		long manualFeederTime = Long.parseLong(config.getString("timer.ManualFeederInterval"))*ONE_HOUR;//Unit of ManualFeederTime: hour
		timer.scheduleAtFixedRate(manualFeederTask, 0, manualFeederTime);
		
		
		//Twitter twitter = new Twitter(queue);
		//try {
		//    /*Collect URL from twitter feed*/
		//    TimerTask twitterTask = twitter;
		//    twitter.authenticate();
		    
		//    long twitterTime = Long.parseLong(config.getString("timer.TwitterInterval"))*ONE_SEC;
		//    timer.scheduleAtFixedRate(twitterTask, 0, twitterTime);
		    
		//} catch (TwitterException te) {
		//    twitter.deauthenticate();
		//    LOGGER.error("Could not initialize Twitter collector.");
		//}
		
		/*Check all URL if they are active or not*/
		TimerTask checkerTask = new Checker();
		long checkerTime = Long.parseLong(config.getString("timer.CheckerInterval"))*ONE_HOUR;//Unit of CheckerTime: hour
		timer.scheduleAtFixedRate(checkerTask, 0, checkerTime);
	}
	/**
	 * To stop the application, called through the daemon windows service
	 * @param args
	 */
	static void stop(String args[]){
		String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
		System.out.println("Exiting system at " + time);
		System.exit(0);
	}
	
	/**
	 * Get a connection to MySQL database named in madaap.xml file
	 * @return
	 */
	static Connection getMySQLconnection() {
		Connection mysqlconn = null;
		try{
			Class.forName("com.mysql.jdbc.Driver").newInstance();	
		}
		catch(Exception e){
			System.out.println(e.getMessage() +"\nCannot register MySQL to DriverManager");
		}
		try{
			XMLConfiguration config = new XMLConfiguration("config/madaap.xml");
			Properties connectionProp = new Properties();
			connectionProp.put("user",config.getString("database.username"));
			connectionProp.put("password", config.getString("database.password"));
			String connectionPath = new StringBuilder()
			    .append("jdbc:mysql://")
			    .append(config.getString("database.url")).append(":")
			    .append(config.getString("database.port"))
			    .append("/"+config.getString("database.dbname"))
			    .toString();
			mysqlconn = DriverManager.getConnection(connectionPath,connectionProp);
		}
		catch(SQLException e){
			System.out.println(e.getMessage() + "\nCannot connect to MySQL. Check if MySQL is running.");
			System.exit(0);
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		return mysqlconn;
	}
	
	public static String constructPath(String ...components) {
	    StringBuilder pathBuilder = new StringBuilder();
	    pathBuilder.append(components[0]);
	    for (int i=1; i < components.length; ++i) {
	        pathBuilder.append(File.separatorChar).append(components[i]);
	    }
	    return pathBuilder.toString(); 
	}
	
}
