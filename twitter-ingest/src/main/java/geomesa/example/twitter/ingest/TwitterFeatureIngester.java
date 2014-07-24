package geomesa.example.twitter.ingest;

import com.google.common.base.Joiner;
import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.SchemaException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
                FEAT_GEOM + ":" + "Point:srid=4326"
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

    FeatureStore source;
    TwitterParser parser;

    String featureName;

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
        twitterType.getUserData().put("geomesa_index_start_time", FEAT_DTG);
        this.twitterType = twitterType;
        this.useExtendedFeatures = useExtendedFeatures;
    }

    public SimpleFeatureType getSchema() {
        return twitterType;
    }

    public void initialize(String featureName, Map<String, Object> dataStoreParams) throws IOException {
        this.featureName = featureName;

        String indexSchema = (String) dataStoreParams.get("indexSchemaFormat");
        log.info("Getting GeoMesa data store - using schema " + (indexSchema == null ? "DEFAULT" : indexSchema));
        final DataStore ds;
        try {
            ds = DataStoreFinder.getDataStore(dataStoreParams);
            if (ds == null) {
                throw new IOException("No data store returned");
            }
        } catch (IOException e) {
            throw new IOException("Error creating Geomesa datastore ", e);
        }

        try {
            ds.getSchema(twitterType.getTypeName());
            log.info("Geomesa tables exist...");
        } catch (IOException e) {
            // schema doesn't exist, create it
            log.info("Creating Geomesa tables...");
            long startTime = System.currentTimeMillis();

            ds.createSchema(twitterType);

            long createTime = System.currentTimeMillis() - startTime;
            log.info("Created schema in " + createTime + "ms");
        }

        try {
           this.source = (FeatureStore) ds.getFeatureSource(getSchema().getName());
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

        log.info("Beginning ingest of file "+file.getName());

        List<SimpleFeatureCollection> results;

        final FileInputStream fis = new FileInputStream(file);
        try {
            results = parser.parse(fis, file.getName());
        } finally {
            fis.close();
        }

        if (results != null && !results.isEmpty()) {
            final long startTime = System.currentTimeMillis();
            log.info(file.getName() + " - beginning insertion of parsed records into data store");
            long count = 0;
            long errors = 0;
            for (SimpleFeatureCollection collection : results) {
                try {
                    source.addFeatures(collection);
                    count += collection.size();
                } catch (IOException e) {
                    // try each one
                    for(SimpleFeatureIterator iter = collection.features();iter.hasNext(); ) {
                        DefaultFeatureCollection c = new DefaultFeatureCollection(featureName, twitterType);
                        c.add(iter.next());
                        try {
                            source.addFeatures(c);
                            count++;
                        } catch (IOException e2) {
                            log.error("Error inserting feature: " + e2.toString());
                            log.debug("Bad feature: " + c.features().next());
                            errors++;
                        }
                    }
                }
            }
            final long insertTime = System.currentTimeMillis() - startTime;
            log.info(file.getName() + " - added " + count + " features and failed " +
                     errors + "in " + insertTime + "ms");
        } else {
            log.info(file.getName() + " - no features ingested");
        }
    }

}
