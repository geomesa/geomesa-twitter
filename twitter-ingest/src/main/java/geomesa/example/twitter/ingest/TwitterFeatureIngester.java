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

    public static final String FEAT_IN_REPLY_TO_USER_ID = "in_reply_to_user_id";
    public static final String FEAT_IN_REPLY_TO_USER_NAME = "in_reply_to_screen_name";
    public static final String FEAT_IN_REPLY_TO_STATUS = "in_reply_to_status_id";
    public static final String FEAT_HASHTAGS = "hashtags";
    public static final String FEAT_USER_MENTIONS = "user_mentions";
    public static final String FEAT_URLS = "urls";
    public static final String FEAT_SYMBOLS = "symbols";
    public static final String FEAT_MEDIA = "media";
    public static final String FEAT_RETWEETS = "retweet_count";
    public static final String FEAT_IS_RETWEET = "retweeted";
    public static final String FEAT_SOURCE = "source";
    public static final String FEAT_FILTER_LEVEL = "filter_level";
    public static final String FEAT_WITHHELD_COPYRIGHT = "withheld_copyright";
    public static final String FEAT_WITHHELD_COUNTRIES = "withheld_in_contries";
    public static final String FEAT_WITHHELD_SCOPE = "withheld_scope";
    public static final String FEAT_LANGUAGE = "lang";

    public static final String FEATURE_SPEC;
    public static final String EXTENDED_FEATURE_SPEC;

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

        final String[] extendedFeatures = {
                FEATURE_SPEC,
                FEAT_IN_REPLY_TO_USER_ID + ":" + "java.lang.Long",
                FEAT_IN_REPLY_TO_USER_NAME + ":" + "String",
                FEAT_IN_REPLY_TO_STATUS + ":" + "String",
                FEAT_HASHTAGS + ":" + "String",
                FEAT_USER_MENTIONS + ":" + "String",
                FEAT_URLS + ":" + "String",
                FEAT_SYMBOLS + ":" + "String",
                FEAT_MEDIA + ":" + "String",
                FEAT_RETWEETS + ":" + "java.lang.Long",
                FEAT_IS_RETWEET + ":" + "String",
                FEAT_SOURCE + ":" + "String",
                FEAT_FILTER_LEVEL + ":" + "String",
                FEAT_WITHHELD_COPYRIGHT + ":" + "String",
                FEAT_WITHHELD_COUNTRIES + ":" + "String",
                FEAT_WITHHELD_SCOPE + ":" + "String",
                FEAT_LANGUAGE + ":" + "String"
        };
        EXTENDED_FEATURE_SPEC = Joiner.on(",").join(extendedFeatures);
    }

    final SimpleFeatureType twitterType;
    final boolean useExtendedFeatures;

    AccumuloFeatureStore source;
    TwitterParser parser;

    public TwitterFeatureIngester() {
        this(false);
    }

    public TwitterFeatureIngester(boolean useExtendedFeatures) {
        final SimpleFeatureType twitterType;
        try {
            twitterType = DataUtilities.createType("twitter", useExtendedFeatures ? EXTENDED_FEATURE_SPEC : FEATURE_SPEC);
        } catch (SchemaException e) {
            throw new IllegalArgumentException("Bad feature spec", e);
        }
        twitterType.getUserData().put(Constants.SF_PROPERTY_START_TIME, FEAT_DTG);
        this.twitterType = twitterType;
        this.useExtendedFeatures = useExtendedFeatures;
    }

    public SimpleFeatureType getSchema() {
        return twitterType;
    }

    public void initialize(String instanceId, String zookeepers, String user, String password,
                           String tableName, String featureName, String indexSchema, Integer numShards) throws IOException {
        final Map<String, String> geomesaParams = new HashMap<>();
        geomesaParams.put("instanceId", instanceId);
        geomesaParams.put("zookeepers", zookeepers);
        geomesaParams.put("user", user);
        geomesaParams.put("password", password);
        geomesaParams.put("tableName", tableName);
        geomesaParams.put("indexSchemaFormat", indexSchema);

        log.info("Getting GeoMesa data store - using schema " + (indexSchema == null ? "DEFAULT" : indexSchema));
        final AccumuloDataStore ds;
        try {
            ds = (AccumuloDataStore) DataStoreFinder.getDataStore(geomesaParams);
        } catch (IOException e) {
            throw new IOException("Error creating Geomesa datastore ", e);
        }

        log.info("Creating Geomesa tables...");
        long startTime = System.currentTimeMillis();
        if (numShards == null) {
            ds.createSchema(getSchema());
        } else {
            log.info("Using " + numShards + " shards");
            ds.createSchema(getSchema(), numShards);
        }
        long createTime = System.currentTimeMillis() - startTime;
        log.info("Created schema in " + createTime + "ms");

        try {
           this.source = (AccumuloFeatureStore) ds.getFeatureSource(getSchema().getName());
        } catch (IOException e) {
            throw new IOException("Error creating Geomesa feature store ", e);
        }

        this.parser = new TwitterParser(featureName, getSchema(), useExtendedFeatures);

        log.info("Initialization complete");
        this.initialized = true;
    }


    public void ingest(final File file) throws IOException, IllegalStateException {
        if(!initialized){
            throw new IllegalStateException("Initialize ingester before using");
        }

        log.info("Begining ingest of file "+file.getName());

        SimpleFeatureCollection results;

        final FileInputStream fis = new FileInputStream(file);
        try {
            results = parser.parse(fis, file.getName());
        } finally {
            fis.close();
        }

        if(results != null && results.size() > 0) {
            final long startTime = System.currentTimeMillis();
            log.info(file.getName() + " - beginning insertion of parsed records into accumulo");
            source.addFeatures(results);
            final long insertTime = System.currentTimeMillis() - startTime;
            log.info(file.getName() + " - added " + results.size() + " features in " + insertTime + "ms");
        } else {
            log.info(file.getName() + " - no features ingested");
        }
    }

}
