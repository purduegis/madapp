package crawler;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import model.UrlSource;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import extractor.Extractor;

public class EntityController {
    private static final Logger LOGGER = Logger.getLogger(EntityController.class);
    private static final List<URL> crawledUrls = new LinkedList<URL>();

	public static synchronized void addCrawledUrl(String url) throws MalformedURLException {
	    crawledUrls.add(new URL(url));
	    LOGGER.debug("Added URL: "+url+" ["+crawledUrls.size()+"]");
	}
	
	public static void begin(UrlSource parentUrlSource) {
		String crawlStorageFolder = "Depth2/";
        int numberOfCrawlers = 1;

        CrawlConfig config = new CrawlConfig();
        config.setCrawlStorageFolder(crawlStorageFolder);
        config.setMaxDepthOfCrawling(1);

        try {
            /*
             * Instantiate the controller for this crawl.
             */
            PageFetcher pageFetcher = new PageFetcher(config);
            RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
            RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
            CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);
            
            /*
             * For each crawl, you need to add some seed urls. These are the first
             * URLs that are fetched and then the crawler starts following links
             * which are found in these pages
             */
            List<String> domains = new ArrayList<String>();
            String strUrl = parentUrlSource.getUrl().toString();
            controller.addSeed(strUrl);
            URL myurl = new URL(strUrl);
            domains.add(myurl.getProtocol() + "://"+ myurl.getHost());
            LOGGER.debug("Seed: " + strUrl);
            LOGGER.debug("Domain: " + domains.toString());
        	controller.setCustomData((List<String>)domains);
    
            /*
             * Start the crawl. This is a blocking operation, meaning that your code
             * will reach the line after this only when crawling is finished.
             */
            controller.start(EntityCrawler.class, numberOfCrawlers);
            
            LOGGER.info("Crawling done. Found "+crawledUrls.size()+" Urls.");
            // Now for each URL in the crawledUrlList call the Extractor.getEntities
            for (URL url : crawledUrls) {
                Extractor.getEntities(new UrlSource(parentUrlSource.getUrl(), 
                                                    url,
                                                    parentUrlSource.getSource()),
                                      false);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}

}
