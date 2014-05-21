package geomesa.example.twitter.ingest;

import com.google.common.base.Joiner;
import geomesa.core.data.AccumuloDataStore;
import geomesa.core.data.AccumuloFeatureStore;
import geomesa.core.index.Constants;
import org.apache.log4j.Logger;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.SchemaException;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TwitterFeatureIngester {

    private static final Logger log = Logger.getLogger(TwitterFeatureIngester.class);

    private boolean initialized = false;

    // Simple Feature attribute names - use only alphanumeric + underscore
    public static final String FEAT_USER_NAME = "user_name";
    public static final String FEAT_USER_ID = "user_id";
    public static final String FEAT_TEXT = "text";
    public static final String FEAT_GEOM = "geom";
    public static final String FEAT_TWEET_ID = "tweet_id";
    public static final String FEAT_DTG = "dtg";

    public static final String FEATURE_SPEC;
    static {
        final String[] features = {
                FEAT_TWEET_ID + ":" + "java.lang.Long",
                FEAT_USER_NAME + ":" + "String",
                FEAT_USER_ID + ":" + "java.lang.Long",
                FEAT_TEXT + ":" + "String",
                FEAT_DTG + ":" + "Date",
                FEAT_GEOM + ":" + "Geometry:srid=4326"
        };
        FEATURE_SPEC = Joiner.on(",").join(features);
    }

    final SimpleFeatureType twitterType;

    AccumuloFeatureStore source;
    TwitterParser parser;

    public TwitterFeatureIngester() {
        final SimpleFeatureType twitterType;
        try {
            twitterType = DataUtilities.createType("twitter", FEATURE_SPEC);
        } catch (SchemaException e) {
            throw new IllegalArgumentException("Bad feature spec", e);
        }
        twitterType.getUserData().put(Constants.SF_PROPERTY_START_TIME, FEAT_DTG);
        this.twitterType = twitterType;
    }

    public SimpleFeatureType getSchema() {
        return twitterType;
    }

    public void initialize(String instanceId, String zookeepers, String user, String password, String tableName, String featureName) throws IOException {
        final Map<String, String> geomesaParams = new HashMap<>();
        geomesaParams.put("instanceId", instanceId);
        geomesaParams.put("zookeepers", zookeepers);
        geomesaParams.put("user", user);
        geomesaParams.put("password", password);
        geomesaParams.put("tableName", tableName);

        log.info("Creating Geomesa tables...");
        final AccumuloDataStore ds;
        try {
            ds = (AccumuloDataStore) DataStoreFinder.getDataStore(geomesaParams);
        } catch (IOException e) {
            throw new IOException("Error creating Geomesa datastore ", e);
        }
        ds.createSchema(getSchema());

        try {
           this.source = (AccumuloFeatureStore) ds.getFeatureSource(getSchema().getName());
        } catch (IOException e) {
            throw new IOException("Error creating Geomesa feature store ", e);
        }

        this.parser = new TwitterParser(featureName, getSchema());

        log.info("Initialization complete");
        this.initialized = true;
    }


    public void ingest(final File file) throws IOException, IllegalStateException {
        if(!initialized){
            throw new IllegalStateException("Initialize ingester before using");
        }

        log.info("Begining ingest of file "+file.getName());

        SimpleFeatureCollection results;

        final long start = System.currentTimeMillis();
        final FileInputStream fis = new FileInputStream(file);
        try {
            results = parser.parse(fis, file.getName());
        } finally {
            fis.close();
        }
        final long time = System.currentTimeMillis() - start;

        if(results != null && results.size() > 0){
            log.info("Beginning insertion of parsed records into accumulo for file: "+file.getName());
            source.addFeatures(results);
            log.info("Added " + results.size() + " features from file " + file.getName() + " in " + time + "ms");
        }
        else {
            log.info("No features ingested from file " + file.getName());
        }
    }

}
