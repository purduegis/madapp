package crawler;

import java.net.MalformedURLException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;


public class EntityCrawler extends WebCrawler{
    private static final Logger LOGGER = Logger.getLogger(EntityCrawler.class);
    
	/**
	 * @param args
	 */
	private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g" 
            + "|png|tiff?|mid|mp2|mp3|mp4"
            + "|wav|avi|mov|mpeg|ram|m4v|pdf" 
            + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
	@Override
	public boolean shouldVisit(WebURL url){
		
		@SuppressWarnings("unchecked")
        List<String> domainsToCrawl = (List<String>) this.getMyController().getCustomData();
		String href = url.getURL().toLowerCase();
		if (FILTERS.matcher(href).matches()) {
		      return false;
		}
		for(String domain : domainsToCrawl){
		      if (href.startsWith(domain)) {
		         return true;
		      }
		   }
        return false;
	}
	@Override
	public void visit(Page page){
		String url = page.getWebURL().getURL();
        try {
            EntityController.addCrawledUrl(url);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

}
