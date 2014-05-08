package model;
import java.net.URL;

import com.google.common.base.Optional;


public class UrlSource {
    public enum Source {
        SOCIAL_MEDIA,
        MANUAL_ENTRY
    }
    private final Optional<URL> parentUrl; //Link to parent URL from which this URL was derived by crawling.
    private final URL url;
    private final Source source;
    
    public UrlSource (URL parentUrl, URL url, Source source) {
        this.parentUrl = Optional.fromNullable(parentUrl);
        this.url = url;
        this.source = source;
    }
    
    public Optional<URL> getParentUrl() {
        return this.parentUrl;
    }

    public URL getUrl() {
        return url;
    }

    public Source getSource() {
        return source;
    }
}
