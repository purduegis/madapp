package extractor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import exceptions.MetadataException;

/**
 * Extract metadata for a specified download link.
 * @author asish
 */
public class MetadataExtractor {
    private static final Logger LOGGER = Logger.getLogger(MetadataExtractor.class);
    private static final char[] HEXCHARS = "0123456789ABCDEF".toCharArray();
    private final Document document; //Document object for the page from which the download links
                                     // and metadata information are retrieved.
    private final File outputPath; //root Path where the metadata file is going to be downloaded to.
    private final MessageDigest msgDigest;
    
    public MetadataExtractor(String pageUrl, String outputPath) throws MetadataException {
        Validate.notNull(pageUrl, "Null URL passed");
        Validate.notNull(outputPath, "Output path is null");
        try {
            this.document = Jsoup.connect(pageUrl).get();
            this.outputPath = new File(outputPath);
            this.msgDigest = MessageDigest.getInstance("SHA");
        } catch (NoSuchAlgorithmException e) {
            throw new MetadataException("Invalid digest algorithm");
        }
        catch (IOException e) {
            throw new MetadataException("Could not connect to URL:" +pageUrl);
        }
        
        //check the output path exists.
        checkPathValidity(this.outputPath);
    }
    
    public String getMetadataUrl(String downloadLink) throws MetadataException {
        //First locate the downloadLink within the page and extract the corresponding Element object.
        Elements links = document.getElementsByAttributeValue("href", downloadLink);
        String metadataUrl = null;

        for (Element downloadElement : links) {
            metadataUrl = getMetadataLinkForDownloadElement(downloadElement);
            if (null != metadataUrl) {
                break;
            }
        }
        if (null == metadataUrl) {
            throw new MetadataException("unable to retrieve metadata for link: "+downloadLink);
        }
        
        LOGGER.debug("Metadata URL: "+metadataUrl);
        return metadataUrl; 
    }
    
    /**
     *  Downloads the metadata specified by the URL to the output path.
     *  The filename of the downloaded metadata file has the following format:
     *  <UID>_downloadLink 
     * @param metadataLink
     * @throws MetadataException 
     */
    public File saveMetadata(String downloadLink, String metadataLink) throws MetadataException {
        BufferedReader urlReader = null;
        PrintWriter writer = null;
        String fileName = new StringBuilder()
                        .append(outputPath)
                        .append(File.separatorChar)
                        .append(generateMetadataFilename(downloadLink))
                        .append('_')
                        .append(getNameComponent(downloadLink))
                        .append(".txt")
                        .toString();
        
        LOGGER.info("Saving metadata to file: "+fileName);
        
        File outputFile = new File(fileName);
        //Create reader to read metadata form metadata URL
        try {
            URL metadataUrl = new URL(metadataLink);
            urlReader = new BufferedReader(new InputStreamReader(metadataUrl.openStream()));
            //Create a writer to write the meatadata to file
            writer = new PrintWriter(outputFile);
            String line;
            while((line = urlReader.readLine()) != null) {
                writer.write(line);
                writer.println();
            }
            LOGGER.info("Saved metadata successfully.");
        }
        catch (IOException e) {
            throw new MetadataException(e);
        }
        finally {
            if (null != writer) {
                writer.close();
            }
            if (null != urlReader) {
                IOUtils.closeQuietly(urlReader);
            }
        }
        return outputFile;
    }

    public File getRootPath() {
        return this.outputPath.getAbsoluteFile();
    }
    
    private String generateMetadataFilename(String downloadLink) {
        msgDigest.update(downloadLink.getBytes());
        byte[] digest = msgDigest.digest();
        msgDigest.reset();
        return byteArrayToHexString(digest);
    }
    
    private static String getNameComponent(String downloadLink) throws MetadataException {
        Pattern pattern = Pattern.compile("(^.*/)(.*$)");
        Matcher matcher = pattern.matcher(downloadLink);
        String filename = null;
        if (matcher.find()) {
            filename = matcher.group(2);
            filename = filename.replaceAll("[.].*?$", ""); //remove the extension from filename
        }
        else {
            throw new MetadataException("Could not find filename from download link: "+downloadLink);
        }
        return filename;
    }
    
    private void checkPathValidity(File path) throws MetadataException {
        if (! (path.exists() && path.isDirectory()) ) {
            throw new MetadataException("The path: "+path+" is not accessible.");
        } 
    }
    
    private String getMetadataLinkForDownloadElement(Element downloadElement) throws MetadataException {
        String metadataLink = null;
        Element parentElement = downloadElement.parent().parent();
        Elements linkElements = parentElement.getElementsByAttribute("href");
        for (Element linkElem: linkElements) {
            Elements metadataElements = linkElem.getElementsContainingText("metadata");
            if (metadataElements.size() == 1) {
                metadataLink = metadataElements.get(0).attr("href");
            } else {
                // Browse through the metadata list and match by name
                // TODO: Currently the match logic is simplistic which matches the name of metadata
                // with that of the name of the download file by ignoring the case and returns the
                // first match.
                String downloadElementName = getNameComponent(downloadElement.attr("href"));
                for (Element elem : metadataElements) {
                    String metadataName = getNameComponent(elem.attr("href"));
                    if (metadataName.equalsIgnoreCase(downloadElementName)) {
                        metadataLink = elem.attr("href");
                        break;
                    }
                }
            }

        }
        return metadataLink;
    }
    
    private static String byteArrayToHexString(byte[] bytes) {
        char [] hexString = new char[bytes.length*2]; //Each byte is 2 hex character
        for (int i = 0; i < bytes.length; ++i) {
            int intVal = bytes[i] & 0xFF;
            hexString[i*2] = HEXCHARS[intVal >>> 4];
            hexString[i*2 + 1] = HEXCHARS[intVal & 0x0F];
            
        }
        return new String(hexString); 
    }
    
}
