package model;

import java.io.File;
import java.net.URL;

import com.google.common.base.Optional;
/**
 * Ecapsulates the database record for the table tab_links which stores the
 * download link for a Gis dataset and its corresponding metadata.
 * @author asish
 *
 */
public class GisDatasetDatabaseRecord {
    private Optional<Integer> id; //The Id of the metadata link record in the database.
    private final URL url; //The download Url for the GIS dataset.
    private Optional<Boolean> isNew; //If the record exists in the DB or if its a new record.
    private Optional<File> metadataFile; //local metadata file.
    
    public GisDatasetDatabaseRecord(Integer id, URL url, Boolean isNew, File metadataFile) {
        this.id = Optional.fromNullable(id);
        this.url = url;
        this.isNew = Optional.fromNullable(isNew);
        this.metadataFile = Optional.fromNullable(metadataFile);
    }

    public Optional<Integer> getId() {
        return id;
    }

    public URL getUrl() {
        return url;
    }

    public Optional<Boolean> isNew() {
        return isNew;
    }
    
    public Optional<File> getMetadataFile() {
        return this.metadataFile;
    }

    public void setId(Integer id) {
        this.id = Optional.of(id);
    }

    public void setNew(Boolean isNew) {
        this.isNew = Optional.of(isNew);
    }
    
    public void setMetadataFile(File metadata) {
        this.metadataFile = Optional.fromNullable(metadata);
    }
}
