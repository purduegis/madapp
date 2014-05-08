package extractor;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import exceptions.MetadataException;
import gate.Annotation;
import gate.AnnotationSet;
import gate.Corpus;
import gate.Document;
import gate.DocumentContent;
import gate.Factory;
import gate.FeatureMap;
import gate.annotation.AnnotationSetImpl;
import gate.creole.ANNIETransducer;
import gate.creole.ExecutionException;
import gate.creole.POSTagger;
import gate.creole.ResourceInstantiationException;
import gate.creole.SerialAnalyserController;
import gate.creole.annotdelete.AnnotationDeletePR;
import gate.creole.gazetteer.DefaultGazetteer;
import gate.creole.morph.Morph;
import gate.creole.orthomatcher.OrthoMatcher;
import gate.creole.splitter.SentenceSplitter;
import gate.creole.tokeniser.DefaultTokeniser;
import gate.util.InvalidOffsetException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import model.GisDatasetDatabaseRecord;
import model.UrlSource;
import utils.IOUtils;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.log4j.Logger;

import crawler.EntityController;

/**
 * Contains methods to perform annotations and extract few entities like author,title etc from a given URL
 * and store them in a MySQL database.
 * It consumes URLs from a FIFO Queue.
 * Queue is being populated by manual feeder and twitter posts, and other collectors from package collectURL
 * @author abhinav
 *
 */
public class Extractor implements Runnable{
    private static final Logger LOGGER = Logger.getLogger(Extractor.class);
    /*declare GATE processing resources*/
    private static AnnotationDeletePR deletor;
    private static DefaultTokeniser tokeniser;
    private static DefaultGazetteer gazetteer;
    private static SentenceSplitter splitter;
    private static POSTagger tagger;
    private static ANNIETransducer transducer;
    private static OrthoMatcher matcher;
    private static Morph morpher;
    private static SerialAnalyserController controller;
    private static FeatureMap map = Factory.newFeatureMap();
    private static FeatureMap pub = Factory.newFeatureMap();
    private static FeatureMap mapAbstract = Factory.newFeatureMap();
    private static Corpus corpus;
    
    static{
        try {
            corpus = Factory.newCorpus("corpus madaap");
        } catch (ResourceInstantiationException e) {
            System.out.println("Could not create corpus");
            e.printStackTrace();
        }
    }

    static{
        try{
            deletor = (AnnotationDeletePR)Factory.createResource("gate.creole.annotdelete.AnnotationDeletePR");
            tokeniser = (DefaultTokeniser) Factory.createResource("gate.creole.tokeniser.DefaultTokeniser");
            gazetteer = (DefaultGazetteer) Factory.createResource("gate.creole.gazetteer.DefaultGazetteer");
            splitter = (SentenceSplitter) Factory.createResource("gate.creole.splitter.SentenceSplitter"); 
            tagger = (POSTagger) Factory.createResource("gate.creole.POSTagger");
            transducer = (ANNIETransducer) Factory.createResource("gate.creole.ANNIETransducer");
            matcher = (OrthoMatcher) Factory.createResource("gate.creole.orthomatcher.OrthoMatcher");
            morpher = (Morph) Factory.createResource("gate.creole.morph.Morph");
            controller = (SerialAnalyserController) Factory.createResource("gate.creole.SerialAnalyserController");
        }
        catch(ResourceInstantiationException e){
            e.printStackTrace();
            System.out.println("Couldn't initialize extractor");
        }
    }

    /*declare queue for collecting URL*/
    private final BlockingQueue<UrlSource> queue;

    /*to start extractor thread*/
    public Extractor(BlockingQueue<UrlSource> q){
        queue = q;
        new Thread(this).start();
    }
    /**
     * This method is slightly long and can/should be broken into smaller modules.
     * 
     * @param url - URL extracted from queue from which we need to extract entities.
     * @param fromQueue - is this URL inserted by a collector. True - add outgoing links to queue. False - Don't add outgoing links
     * @return
     * @throws IOException 
     * @throws SQLException 
     */
    public static int getEntities(UrlSource urlSource, boolean fromQueue) throws IOException {

        /*Create a connection to MySQL database*/
        Connection mysqlconn = Madaap.getMySQLconnection();
        
        LOGGER.info("Processing URL: "+urlSource.getUrl().toString());

        /*Check if URL already exists in database*/
        if (isURLexisting(urlSource.getUrl(), mysqlconn)){
            IOUtils.closeQuitely(mysqlconn);
            return 0;
        }

        /*Create new GATE document from URL*/
        Document doc = createDocument(urlSource.getUrl());

        if (doc == null) { 
            IOUtils.closeQuitely(mysqlconn);
            return 1;
        }

        /*Get original HTML markups of GATE document*/
        AnnotationSet original = doc.getAnnotations("Original markups");

        /*Get downloadable links*/
        Set<URL> datasetUrlList = getDownloadLinks(original, doc, urlSource.getUrl());

        /*If current document is a sub-page and has no links, discard document*/
        if(fromQueue==false && datasetUrlList.isEmpty()){
            Factory.deleteResource(doc);
            IOUtils.closeQuitely(mysqlconn);
            return 0;
        }
        
        /*Perform annotation using GATE NLP tools on GATE document*/
        AnnotationSet set = doAnnotation(doc);
        if (set == null){
            IOUtils.closeQuitely(mysqlconn);
            return 0;
        }

        URL metadataurl = hasMetadata(original, doc, urlSource.getUrl());

        /*If web page has metadata, extract spatial extent from metadata page*/

        Set<String> spatialExtent = new HashSet<String>();
        if (metadataurl != null){
            spatialExtent = getSpatialExtent(metadataurl);
        }
        
        /*Get all the entities extracted from web page*/
        Set<String> formatSet = getFormats(datasetUrlList);
        Set<String> titleSet = getTitle(original,doc);
        Set<DocumentContent> authorSet = getAuthor(set,original,doc);
        String abst = getAbstract(original,doc);

        /*If download links is empty, crawl sub-pages i.e. Call EntityController using current URL as seed
         * Call the crawler only if current URL is taken from input queue*/

        if (crawlSubpages(urlSource, fromQueue, doc, original, datasetUrlList)) {
            IOUtils.closeQuitely(mysqlconn);
            return 0;
        }
        
        // Save the metadata for download links.
        //TODO: This replaces the existing mechanism for getting metadata. See if the metadata related tables/
        // columns need to be dropped.
        
        Collection<GisDatasetDatabaseRecord> metadataRecords = getGisDatasetRecords (
                urlSource, mysqlconn, datasetUrlList);

        writeToDatabase(mysqlconn, urlSource,
                spatialExtent, formatSet, titleSet, authorSet, abst,
                metadataRecords);
        
        IOUtils.closeQuitely(mysqlconn);
        
        /*Clean up GATE resources*/
        Factory.deleteResource(doc);
        return 0;
    }
    /**
     * Writes metadata, download Url among other information to the database for each source URL.
     * The data is written in a single transaction meaning that the database is committed only
     * when data is written successfully to all the tables. Otherwise nothing is committed. 
     * @param mysqlconn
     * @param urlSource
     * @param spatialExtent
     * @param formatSet
     * @param titleSet
     * @param authorSet
     * @param abst
     * @param metadataRecords
     */
    private static void writeToDatabase(Connection mysqlconn,
            UrlSource urlSource, 
            Set<String> spatialExtent,
            Set<String> formatSet,
            Set<String> titleSet,
            Set<DocumentContent> authorSet,
            String abst,
            Collection<GisDatasetDatabaseRecord> metadataRecords) {
        
        try {
            mysqlconn.setAutoCommit(false);
            
            /*insert URL in database*/
            insertURL(mysqlconn, urlSource);

            /*Insert other entities in database*/
            Statement getURLid =  mysqlconn.createStatement();
            ResultSet urlID = getURLid.executeQuery("select ID from tab_url where URL = '"+
                    urlSource.getUrl().toString() +
                    "'limit 1");
            if (urlID.next()) {
                File outputPath = new File(Madaap.config.getString("MetadataPath"));
                int id = urlID.getInt(1);
                insertAbstract(mysqlconn,abst,id);
                insertFormat(mysqlconn,formatSet,id);
                insertAuthor(mysqlconn,authorSet,id);
                insertTitle(mysqlconn,titleSet,id);
                insertLink(mysqlconn,metadataRecords, outputPath, id);
                insertSpatialExtent(mysqlconn,spatialExtent,id);
                LOGGER.info("URL "+urlSource.getUrl().toString()+" inserted with ID: "+id);
            }
            
            IOUtils.closeQuitely(getURLid);
            mysqlconn.commit();
            
        } catch(SQLException e) {
            try {
                LOGGER.fatal("Caught exception. Rollingback.", e);
                mysqlconn.rollback();
            } catch (SQLException se) {
                LOGGER.error("Error in rolling back database.", se);
                throw new RuntimeException(se);
            }
        }
        
    }
    
    /**
     * Returns a list of {@link GisDatasetDatabaseRecord} from the source Url and
     * dataset URLs
     * @param urlSource Source URL from which gis datasets are explored.
     * @param mysqlconn
     * @param datasetUrlList List of dataset URLs
     * @return
     */
    private static Collection<GisDatasetDatabaseRecord> getGisDatasetRecords (
            UrlSource urlSource, Connection mysqlconn, Set<URL> datasetUrlList) {
        MetadataExtractor metadataExtractor = null;
        try {
            metadataExtractor = 
                    new MetadataExtractor(urlSource.getUrl().toString(), Madaap.config.getString("MetadataPath"));
        } catch (MetadataException e) {
            LOGGER.error("Could not initialize metadata extractor.",e);
            throw new RuntimeException(e);
        }
        
        Collection<GisDatasetDatabaseRecord> metadataRecords =
                getMetadataLinkDatabaseRecords(datasetUrlList, mysqlconn);
        
        for (GisDatasetDatabaseRecord record : metadataRecords) {
            String downloadUrl = record.getUrl().toString();
            
            try {
                String metadataUrl = metadataExtractor.getMetadataUrl(downloadUrl);
                File localMetadataFile = 
                        metadataExtractor.saveMetadata(downloadUrl, metadataUrl);
                record.setMetadataFile(localMetadataFile);
                
            } catch (MetadataException e) {
                // Log the exception and continue with other links.
                LOGGER.error("Metadata could not be saved for the link: "+downloadUrl);
                e.printStackTrace();
            }
        }
        return metadataRecords;
    }
    /**
     * Get end markers from download links
     * @param downloadLinks - a set of URLs which end with our GIS download markers
     * @return
     */
    private static Set<String> getFormats(Set<URL> downloadLinks) {
        Set<String> formatSet = new HashSet<String>(); 
        Iterator<URL> linkit = downloadLinks.iterator();
        while(linkit.hasNext()){
            String link = linkit.next().toString();
            formatSet.add(link.substring(link.lastIndexOf('.')+1, link.length()));
        }
        return formatSet;
    }

    /**
     * insert spatial extent in database
     * @param mysqlconn
     * @param spatialExtent
     * @param id
     * @throws SQLException
     */
    private static void insertSpatialExtent(Connection mysqlconn,
            Set<String> spatialExtent, int id) throws SQLException {
        PreparedStatement insertSE = mysqlconn.prepareStatement("insert into tab_spatialextent values(?,?,?)");
        Iterator<?> seiter = spatialExtent.iterator();
        while(seiter.hasNext()){
            insertSE.setString(1, null);
            insertSE.setInt(2, id);
            insertSE.setString(3, seiter.next().toString());
            insertSE.executeUpdate();
        }
        insertSE.close();
    }

    /**
     * insert download links in database
     * @param mysqlconn
     * @param datasetList
     * @param urlId
     * @throws SQLException
     */
    private static void insertLink(
            Connection mysqlconn,
            Collection<GisDatasetDatabaseRecord> datasetRecords,
            File metadataRoot,
            int urlId) throws SQLException {
        
        PreparedStatement insertLinks = mysqlconn.prepareStatement("insert into tab_links values(?,?,?,?)");
        PreparedStatement insertUrlLinkMap = 
                mysqlconn.prepareStatement("insert into tab_url_link_map values (?,?,?)");
        
        for (GisDatasetDatabaseRecord record : datasetRecords) {
            if (record.isNew().get() == true) {
                insertLinks.setInt(1, record.getId().get());
                insertLinks.setString(2, record.getUrl().toString());
                if (record.getMetadataFile().isPresent()) {
                    //Store only the relative path to the metadata file
                    File metadataFile = record.getMetadataFile().get();
                    insertLinks.setString(3, metadataFile.getName());
                }
                else {
                    insertLinks.setNull(3, java.sql.Types.VARCHAR);
                }
                insertLinks.setTimestamp(4, null);
                insertLinks.executeUpdate();
            }

            insertUrlLinkMap.setInt(1, urlId);
            insertUrlLinkMap.setInt(2, record.getId().get());
            insertUrlLinkMap.setTimestamp(3, null);
            insertUrlLinkMap.executeUpdate();
        }
        insertLinks.close();
    }
    
    private static Collection<GisDatasetDatabaseRecord> getMetadataLinkDatabaseRecords (
            Collection<URL> datasetList,
            Connection mysqlconn) {
        
        Statement statement = null;
        ResultSet resultSet = null;
        Map<String, GisDatasetDatabaseRecord> dbRecords = new HashMap<String, GisDatasetDatabaseRecord>();
        Integer maxId = 0;
        int newRecords = 0;
        
        for (URL dataset : datasetList) {
            dbRecords.put(dataset.toString(),
                           new GisDatasetDatabaseRecord(null, dataset, null, null));
        } 
        
        try {
            statement = mysqlconn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(1000);
            resultSet = statement.executeQuery("select * from tab_links order by ID");
            
            while (resultSet.next()) {
                maxId = resultSet.getInt(1); //ID. Since the records are sorted by ID the last Id is maxId.
                String downloadUrl = resultSet.getString(2); //Get the URL
                if (dbRecords.containsKey(downloadUrl)) {
                    GisDatasetDatabaseRecord record = dbRecords.get(downloadUrl);
                    record.setNew(false);
                    record.setId(maxId);
                }
            }
            
            for(GisDatasetDatabaseRecord record : dbRecords.values()) {
                if (record.getId().isPresent() == false) {
                    record.setNew(true);
                    record.setId(++maxId);
                    newRecords++;
                }
            }
            LOGGER.info("Found "+newRecords+" new downloadable links");
            
        } catch (SQLException e) {
            LOGGER.error("Error accessing database", e);
            throw new RuntimeException(e);
        }
        finally {
            IOUtils.closeQuitely(resultSet);
            IOUtils.closeQuitely(statement);
        }
        return dbRecords.values();
    }

    /**
     * insert titles in database
     * @param mysqlconn
     * @param titleSet
     * @param id
     * @throws SQLException
     */
    private static void insertTitle(Connection mysqlconn, Set<?> titleSet,int id) throws SQLException {
        PreparedStatement insertTitles = mysqlconn.prepareStatement("insert into tab_title values(?,?,?,?)");
        Iterator<?> Title = titleSet.iterator();
        while(Title.hasNext()){
            insertTitles.setString(1, null);
            insertTitles.setInt(2, id);
            insertTitles.setString(3, Title.next().toString());
            insertTitles.setTimestamp(4, null);
            insertTitles.executeUpdate();
        }
        insertTitles.close();
    }

    /**
     * insert authors in database
     * @param mysqlconn
     * @param authorSet
     * @param id
     * @throws SQLException
     */
    private static void insertAuthor(Connection mysqlconn, Set<?> authorSet,int id) throws SQLException {
        PreparedStatement insertAuthors = mysqlconn.prepareStatement("insert into tab_authors values(?,?,?,?)");
        Iterator<?> author = authorSet.iterator();
        while(author.hasNext()){
            insertAuthors.setString(1, null);
            insertAuthors.setInt(2, id);
            insertAuthors.setString(3, author.next().toString());
            insertAuthors.setTimestamp(4, null);
            insertAuthors.executeUpdate();
        }
        insertAuthors.close();
    }

    /**
     * insert formats in database
     * @param mysqlconn
     * @param formatSet
     * @param id
     * @throws SQLException
     */
    private static void insertFormat(Connection mysqlconn, Set<String> formatSet,int id) throws SQLException {
        PreparedStatement insertFormats = mysqlconn.prepareStatement("insert into tab_format values(?,?,?,?)");
        Iterator<?> format = formatSet.iterator();
        while(format.hasNext()){
            insertFormats.setString(1, null);
            insertFormats.setInt(2, id);
            insertFormats.setString(3, format.next().toString());
            insertFormats.setTimestamp(4, null);
            
            insertFormats.executeUpdate();
        }
        insertFormats.close();
    }

    /**
     * insert abstract into database
     * @param mysqlconn
     * @param abst
     * @param id
     * @throws SQLException
     */
    private static void insertAbstract(Connection mysqlconn, String abst, int id) throws SQLException {
        PreparedStatement insertAbstract = mysqlconn.prepareStatement("insert into tab_abstract values(?,?,?,?)");
        insertAbstract.setString(1, null);
        insertAbstract.setInt(2, id);
        insertAbstract.setString(3, abst);
        insertAbstract.setTimestamp(4, null);
        insertAbstract.executeUpdate();
        insertAbstract.close();
    }

    /**
     * insert main URL in database i.e. the URL from which entities are extracted
     * @param mysqlconn
     * @param url
     * @param source source of the URL viz. manual_entry, social_media
     * @throws SQLException 
     */
    private static void insertURL(Connection mysqlconn,UrlSource urlSource) throws SQLException{
        /*Insert URL in database*/
        PreparedStatement insertURL = null;
        String parentUrl = null;
        if (urlSource.getParentUrl().isPresent()) {
            parentUrl = urlSource.getParentUrl().get().toString();
        }
        try {
            insertURL = mysqlconn.prepareStatement("insert into tab_url values (?,?,?,?,?,?)");
            insertURL.setString(1, null);		/*auto-incremented ID values*/
            insertURL.setString(2, urlSource.getUrl().toString());		/*URL to be inserted into*/
            insertURL.setString(3, parentUrl);
            insertURL.setInt(4, Checker.FROM_EXTRACTOR);
            insertURL.setString(5, urlSource.getSource().name());
            insertURL.setTimestamp(6, null); //Auto-update of timestamp.a
            insertURL.executeUpdate();
        } finally {
            IOUtils.closeQuitely(insertURL);
        }
        LOGGER.debug("URL inserted: " + urlSource.getUrl().toString()+" from: "+urlSource.getSource());
    }

    /**
     * retrieve a set of four elements from an XML file
     * These elements must contain numbers that lie within range -180 to +180
     * These represent longitude and latitude of data set. 
     * @param metadataurl - an XML file
     * @return set of quadruples representing upper and lower limit of longitude and latitude 
     */
    private static Set<String> getSpatialExtent(URL metadataurl) {
        XMLConfiguration config = null;
        List<Object> words = new ArrayList<Object>();

        /*read keywords from configuration file*/
        try {
            config = new XMLConfiguration("config/madaap.xml");
        } catch (ConfigurationException e1) {
            throw new RuntimeException(e1);
        }
        if (config!=null){
            words = config.getList("SpatialExtentKeyword.north");
            words.addAll(config.getList("SpatialExtentKeyword.south"));
            words.addAll(config.getList("SpatialExtentKeyword.east"));
            words.addAll(config.getList("SpatialExtentKeyword.west"));
        }

        /*create GATE document from XML file*/
        Document doc = createDocument(metadataurl);

        /*Iterate through original HTML annotations of document to find those IDs that contain numbers within range -180 to +180*/
        AnnotationSet original = doc.getAnnotations("Original markups");
        Set<String> spatialextent = new HashSet<String>();
        AnnotationSet spatial = new AnnotationSetImpl(doc);
        List<Integer> idlist = new ArrayList<Integer>(); 
        Iterator<Annotation> annit = original.iterator();
        while(annit.hasNext()){
            Annotation ann = annit.next();
            String content = null;
            try {
                content = doc.getContent().getContent(ann.getStartNode().getOffset(), ann.getEndNode().getOffset()).toString();
                content = content.trim();
            } catch (InvalidOffsetException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if (NumberUtils.isNumber(content)& !content.isEmpty()){
                float numericcontent = Float.parseFloat(content);
                if (numericcontent<=180 && numericcontent>=-180){
                    String type = ann.getType().toLowerCase();
                    //if(type.contains("west")||type.contains("east")||type.contains("north")||type.contains("south")){
                    Iterator<Object> wordit = words.iterator();
                    while(wordit.hasNext()){
                        if(type.contains((String)wordit.next())){
                            //System.out.println(ann+"\n"+content);
                            FeatureMap map = Factory.newFeatureMap();
                            map.put("content", content);
                            ann.setFeatures(map);
                            idlist.add(ann.getId());
                            spatial.add(ann);
                            break;
                        }	
                    }
                }
            }
        }
        /*Sort the list of ID of annotations*/
        Collections.sort(idlist);
        if(idlist.size()==0){
            return spatialextent;
        }

        /*Obtain a list of starting annotation ID which form a group of four*/
        List<Integer> startid = getSpatialGroup(idlist);

        /*Iterate through the starting IDs to add quadruples to returning set*/
        Iterator<Integer> idit = startid.iterator();
        while(idit.hasNext()){
            Integer id = idit.next();
            Annotation one = spatial.get(idlist.get(id));
            Annotation two = spatial.get(idlist.get(id+1));
            Annotation three = spatial.get(idlist.get(id+2));
            Annotation four = spatial.get(idlist.get(id+3));
            String first = one.getType() + ": " + one.getFeatures().get("content");
            String second = two.getType() + ": " + two.getFeatures().get("content");
            String third = three.getType() + ": " + three.getFeatures().get("content");
            String fourth = four.getType() + ": " + four.getFeatures().get("content");
            spatialextent.add(first+";"+second+";"+third+";"+fourth);
            System.out.println(first+"\n"+second+"\n"+third+"\n"+fourth+"\n\n");
        }

        /*Clean resource and return*/
        Factory.deleteResource(doc);
        return spatialextent;
    }

    /**
     * Used in conjunction with above method getSpatialExtent
     * e.g.
     * Input:  3,4,5,6,45,46,47,48,53
     * Output: 3,45
     * @param idlist - list of ID of annotations
     * @return - list of integers which form a group of four from the input list of integers
     */
    private static List<Integer> getSpatialGroup(List<Integer> idlist) {
        Integer old = idlist.get(0);
        ArrayList<Integer> startingidlist = new ArrayList<Integer>();
        ArrayList<Integer> diff = new ArrayList<Integer>(idlist.size()-1);
        for(int i=1;i<idlist.size();i++){
            Integer now = idlist.get(i);
            diff.add(now-old);
            old=now;
        }
        Integer position = new Integer(0);
        while(position<diff.size()-2){
            if(diff.get(position).equals(diff.get(position+1))){
                if(diff.get(position+1).equals(diff.get(position+2))){
                    startingidlist.add(position);
                }
            }
            position++;
        }
        return startingidlist;
    }

    /**
     * 
     * @param url
     * @param fromQueue
     * @param doc
     * @param original
     * @param downloadLinks
     * @return
     */
    private static boolean crawlSubpages(UrlSource urlSource, boolean fromQueue, Document doc,
            AnnotationSet original, Set<URL> downloadLinks) {

        if (downloadLinks.isEmpty()){
            if (fromQueue){
                EntityController.begin(urlSource);
            }
            else{
                LOGGER.debug("Sub-page URL,No links,Don't insert in DB");
            }
            Factory.deleteResource(doc);
            return true;
        }
        else{
            return false;	
        }
    }
    /**
     * create a GATE document
     * @param url - URL from which to create document
     * @return
     */
    private static Document createDocument(URL url) {
        Document doc = null;
        try {
            doc = Factory.newDocument(url);
        } catch (ResourceInstantiationException e1) {
            System.out.println("\nCannot connect to provided link." + url);
        }
        return doc;
    }

    /**
     * Check if URL is already in database
     * @param url
     * @param mysqlconn
     * @return
     */
    private static boolean isURLexisting(URL url, Connection mysqlconn) {
        try {
        Statement checkifexists = mysqlconn.createStatement();
        ResultSet check = 
                checkifexists.executeQuery("Select Exists (Select 1 from tab_url where URL = '"+ url.toString() + "')");
        if (check.next())
        {
            if (check.getInt(1) == 1){
                LOGGER.debug(url.toString() + " exists: Going to next");
                return true;
            }
        }
        checkifexists.close();
        check.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }
    /**
     * Check if the webpage identified by URL has an outgoing link which may contain metadata
     * Currently, metadata is an XML file whose anchor tag contain the word 'metadata'
     * @param set
     * @param doc
     * @param url
     * @return
     */
    private static URL hasMetadata(AnnotationSet set,Document doc, URL url){
        String contenttype = new String();
        AnnotationSet linkSet = set.get("a");
        Iterator<Annotation> linkIt = linkSet.iterator();
        while (linkIt.hasNext()){
            Annotation ann = (Annotation) linkIt.next();
            String value = null;
            try {
                value = doc.getContent().getContent(ann.getStartNode().getOffset(), ann.getEndNode().getOffset()).toString();
            } catch (InvalidOffsetException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if (value.toLowerCase().contains("metadata")||value.toLowerCase().contains("xml")){
                if (!ann.getFeatures().keySet().contains("href")){
                    continue;
                }
                String metadatahref = (String) ann.getFeatures().get("href");
                String metadataurl = null;
                if (metadatahref.startsWith("/")){
                    metadataurl = url.getProtocol() + "://" + url.getHost() + metadatahref;
                }
                else{
                    //metadataurl = url.toString().substring(0, url.toString().lastIndexOf('/')+1)+metadatahref;
                    metadataurl = metadatahref;
                }
                URL u = null;
                try {
                    u = new URL(metadataurl);
                } catch (MalformedURLException e) {
                    LOGGER.warn("Ignoring Malformed URL: "+metadataurl);
                    continue;
                }
                URLConnection uc = null;
                try {
                    uc = u.openConnection();
                } catch (IOException e) {
                    LOGGER.warn("Could not connect to: "+metadataurl+". Ignoring...");
                    continue;
                }
                contenttype = uc.getContentType();
                if(null != contenttype && contenttype.endsWith("/xml")){
                    //System.out.println(u+"\n"+contenttype);
                    return u;
                }
            }
        }
        return null;
    }


    /**
     * Perform annotation on GATE document using NLP toold provided by GATE
     * @param doc - document on which annotation is done
     * @return set of annotations
     * @throws Exception
     */
    public static AnnotationSet doAnnotation(Document doc){
        System.out.println("annotating...");
        AnnotationSet set = null;
        corpus.add(doc);
        controller.add(deletor);
        controller.add(tokeniser);
        controller.add(gazetteer);
        controller.add(splitter);
        controller.add(tagger);
        controller.add(transducer);
        controller.add(matcher);
        controller.add(morpher);
        controller.setCorpus(corpus);
        try {
            controller.execute();
            set = doc.getAnnotations();
        } catch (ExecutionException e) {
            System.out.println("Failed to execute processing pipeline");
            return null;
        }
        finally{
            corpus.remove(doc);
            corpus.unloadDocument(doc);
        }
        return set;
    }


    /**
     * 
     * @param set
     * @param doc
     * @return
     */
    public static Set<DocumentContent> getFormats(AnnotationSet set, Document doc){
        //FeatureMap map = Factory.newFeatureMap();
        map.put("majorType", "GISformat");
        AnnotationSet formats = set.get("Lookup",map);
        Set<DocumentContent> formatSet = new HashSet<DocumentContent>();
        Iterator<Annotation> it = formats.iterator();
        while(it.hasNext()){
            Annotation ann = it.next();
            try {
                formatSet.add(doc.getContent().getContent(ann.getStartNode().getOffset(), ann.getEndNode().getOffset()));
            } catch (InvalidOffsetException e) {
                e.printStackTrace();
            }
        }
        return formatSet;
    }
    /**
     * 
     * @param set
     * @param doc
     * @return
     * @throws InvalidOffsetException
     */
    public static Set<String> getTitle(AnnotationSet set,Document doc){
        Set<String> titles = new HashSet<String>();
        titles.add("title");
        titles.add("h1");
        //titles.add("h2");
        AnnotationSet original = set.get(titles);
        Set<String> titleSet = new HashSet<String>();
        if(original.isEmpty()){
            titleSet.add("none");
        }
        else{
            Iterator<Annotation> titleIt = original.iterator();
            int i=0;
            while (titleIt.hasNext()){
                Annotation ann = titleIt.next();
                try {
                    titleSet.add(doc.getContent().getContent(ann.getStartNode().getOffset(), ann.getEndNode().getOffset()).toString());
                } catch (InvalidOffsetException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if ((i++)==5) break;
            }
        }
        return titleSet;
    }
    /**
     * 
     * @param set
     * @param original
     * @param doc
     * @return
     */
    public static Set<DocumentContent> getAuthor(AnnotationSet set,AnnotationSet original, Document doc){

        AnnotationSet org = set.get("Organization");
        Set<DocumentContent> orgSet1 = new HashSet<DocumentContent>();
        Set<DocumentContent> orgSet2 = new HashSet<DocumentContent>();
        Set<DocumentContent> orgSet3 = new HashSet<DocumentContent>();
        Set<DocumentContent> orgSet = new LinkedHashSet<DocumentContent>();

        /*Part 1*/
        Annotation ann = null;
        Iterator<Annotation> it = org.iterator();
        while(it.hasNext()){
            ann = (Annotation) it.next();
            try {
                orgSet1.add(doc.getContent().getContent(ann.getStartNode().getOffset(), ann.getEndNode().getOffset()));
            } catch (InvalidOffsetException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        /*Part-2*/
        AnnotationSet sentence = set.get("Sentence");
        //FeatureMap pub = Factory.newFeatureMap();
        //String list = new String("distribute issue produce publish release assemble supply provide create develop");
        pub.put("root", "publish");
        pub.put("root", "distribute");
        pub.put("root", "distribution");
        pub.put("root", "produce");
        pub.put("root", "provide");
        pub.put("root", "create");
        AnnotationSet allContained = null;
        AnnotationSet pubTokens = null;
        Iterator<Annotation> senit = sentence.iterator();
        while(senit.hasNext()){
            ann = (Annotation) senit.next();
            allContained = set.get(ann.getStartNode().getOffset(), ann.getEndNode().getOffset());
            pubTokens = allContained.get("Token", pub);
            if(!pubTokens.isEmpty()){
                allContained = set.get("Organization",ann.getStartNode().getOffset(), ann.getEndNode().getOffset());
                Iterator<Annotation> orgIt = allContained.iterator();
                while (orgIt.hasNext()){
                    ann = (Annotation) orgIt.next();
                    try {
                        orgSet2.add(doc.getContent().getContent(ann.getStartNode().getOffset(), ann.getEndNode().getOffset()));
                    } catch (InvalidOffsetException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }

        /*Part-3*/
        AnnotationSet title = original.get("title");
        AnnotationSet orgsInTitles = null;
        if (!title.isEmpty()){
            Annotation titleann = (Annotation) title.toArray()[0];
            orgsInTitles = set.get("Organization",titleann.getStartNode().getOffset(), titleann.getEndNode().getOffset());
            Iterator<Annotation> orgInTitle = orgsInTitles.iterator();
            while (orgInTitle.hasNext()){
                ann = (Annotation) orgInTitle.next();
                try {
                    orgSet3.add(doc.getContent().getContent(ann.getStartNode().getOffset(), ann.getEndNode().getOffset()));
                } catch (InvalidOffsetException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        /*Combine sets*/
        orgSet.addAll(orgSet3);
        orgSet.addAll(orgSet2);
        //orgSet.addAll(orgSet1);
        return orgSet;
    }
    /**
     * 
     * @param set
     * @param doc
     * @return
     */
    public static String getAbstract(AnnotationSet set,Document doc){
        mapAbstract.put("class", "body");
        XMLConfiguration config = null;
        try {
            config = new XMLConfiguration("config/madaap.xml");
        } catch (ConfigurationException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        int minAbstractLength = config.getInt("Abstract.MinimumLength");
        AnnotationSet para = set.get("p");
        if (para.isEmpty()){
            return "None found";
        }
        else{
            Iterator<Annotation> it = para.iterator();
            List<Integer> idlist = new ArrayList<Integer>();
            while(it.hasNext()){
                Annotation ann = it.next();
                Long length = ann.getEndNode().getOffset() - ann.getStartNode().getOffset();
                if(length>=minAbstractLength){
                    idlist.add(ann.getId());	
                }
            }
            if(idlist.size()==0){
                return "None found";
            }
            Collections.sort(idlist);
            Annotation ann = ((AnnotationSetImpl)set).get(idlist.get(0));
            String content = null;
            try {
                content = doc.getContent().getContent(ann.getStartNode().getOffset(), ann.getEndNode().getOffset()).toString();
            } catch (InvalidOffsetException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return content;
            /*
			Iterator<Annotation> it = para.iterator();
			Annotation ann = (Annotation) para.toArray()[0];
			int minVal = ann.getId().intValue();
			int current = 0;
			while(it.hasNext()){
				ann = (Annotation) it.next();
				current = ann.getId().intValue();
				if (current<minVal){
					minVal = current;
				}
			}
			ann = para.get(minVal);
			String abstractValue = "Empty";
			try {
				abstractValue = doc.getContent().getContent(ann.getStartNode().getOffset(),ann.getEndNode().getOffset()).toString();
			} catch (InvalidOffsetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
			return abstractValue;*/
        }
    }
    /**
     * 
     * @param original
     * @param doc
     * @param url
     * @return
     */
    public static Set<URL> getDownloadLinks(AnnotationSet original,Document doc, URL url){
        Set<String> href = new HashSet<String>();
        href.add("href");
        AnnotationSet a = original.get("a",href);
        Annotation ann = null;
        Set<URL> linkSet = new HashSet<URL>();
        String linkVal = null;
        Iterator<Annotation> links = a.iterator();

        XMLConfiguration config = null;
        Set<String> downloadTypes = getListFromConfig(config, "downloads.download");
        Set<String> downloadMimeTypes = getListFromConfig(config, "downloadMimeTypes.downloadMimeType");

        HttpClient httpClient = new DefaultHttpClient();
        HttpParams params = httpClient.getParams();
        HttpConnectionParams.setConnectionTimeout(params, 10000);
        HttpConnectionParams.setSoTimeout(params, 10000);
        
        URL absoluteUrl = null;
        Pattern pattern = Pattern.compile("(^.*Content-Type:\\s)(.*$)");
        
        while (links.hasNext()) {
            ann = (Annotation) links.next();
            linkVal = (String) ann.getFeatures().get("href");
            Iterator<String> downloadTypesIterator = downloadTypes.iterator();
            try {
                absoluteUrl = addAbsoluteUrl(url, linkSet, linkVal);
            } catch (MalformedURLException e1) {
                LOGGER.debug("Ignoring malformed URL: "+url+":"+linkVal);
                continue;
            }
            
            while (downloadTypesIterator.hasNext()){
                String downloadType = downloadTypesIterator.next();
                if (linkVal.toLowerCase().endsWith(downloadType)){
                    LOGGER.debug("Found downloadable link: "+absoluteUrl); 
                    linkSet.add(absoluteUrl);
                    break;
                } else if (linkVal.contains(downloadType)) {
                    // Check MIME type to make sure that the link is downloadable.
                    String contentType = getMimeTypeFromHeader(httpClient, absoluteUrl, pattern);
                    if (downloadMimeTypes.contains(contentType)) {
                        LOGGER.debug("Content Type: "+contentType);
                        linkSet.add(absoluteUrl);
                        LOGGER.debug("Found downloadable link MIME type: "+absoluteUrl);
                        break;
                    }
                }
            }
        }
        return linkSet;

    }
    
    private static String getMimeTypeFromHeader(HttpClient httpClient,
                                                URL absoluteUrl,
                                                Pattern pattern) {
        try {
            LOGGER.info("MIME: "+absoluteUrl);
            HttpHead request = new HttpHead(absoluteUrl.toString());
            HttpResponse response = httpClient.execute(request);
            Header [] headers = response.getAllHeaders();
            for (Header header : headers) {
                Matcher matcher = pattern.matcher(header.toString());
                if (matcher.matches()) {
                    String contentType = matcher.group(2);
                    return contentType;
                }
            } 
        } catch (Exception e) {
            LOGGER.error("Error accessing URl: "+absoluteUrl);
            LOGGER.error("Could not determine content type of the URL. Ignoring.", e);
        }
        return "";
    }
    
    private static URL addAbsoluteUrl(URL url, Set<URL> linkSet, String linkVal)
            throws MalformedURLException {
        String base = url.toString().substring(0, url.toString().lastIndexOf('/'));
            URL baseurl = new URL(base);
            return new URL(baseurl,linkVal);
    }
    
    private static Set<String> getListFromConfig(XMLConfiguration config, String configKey) {
        Set<String> downloadTypes = Collections.emptySet();
        try {
            config = new XMLConfiguration("config/madaap.xml");
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
        if (config != null){
            List<Object> entries = config.getList(configKey);
            downloadTypes = new HashSet<String>(entries.size());
            for (Object entry : entries) {
                downloadTypes.add((String)entry);
            }
        }
        return downloadTypes;
    }

    public void run() {
        try {
            while(true){
                System.out.println("Extractor launched");
                UrlSource urlSource = queue.take();
                if(getEntities(urlSource,true)==0){
                    report();
                }
            }
        } catch (Exception e) {
            LOGGER.fatal("Caught exception.", e);
            e.printStackTrace();
            return;
        }
    }


    private void report() {
        // Report to user and Admin
        System.out.println("Extractor successfully stored entities.\n");
        //System.exit(0);
    }
}
