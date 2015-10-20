/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.example.twitter.ingest;

import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.DefaultFeatureCollection;
import org.locationtech.geomesa.accumulo.data.tables.AvailableTables;
import org.locationtech.geomesa.accumulo.util.SftBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

    public static SimpleFeatureType buildBasic(String name) {
        return new SftBuilder()
                .stringType(FEAT_TWEET_ID, false)
                .stringType(FEAT_USER_NAME, true)
                .stringType(FEAT_USER_ID, true)
                .stringType(FEAT_TEXT, false)
                .date(FEAT_DTG, true)
                .point(FEAT_GEOM, true)
                .withIndexes(AvailableTables.Z3TableSchemeStr())
                .build(name);
    }

    public static SimpleFeatureType buildExtended(String name) {
        return new SftBuilder()
                .stringType(FEAT_TWEET_ID, false)
                .stringType(FEAT_USER_NAME, true)
                .stringType(FEAT_USER_ID, true)
                .stringType(FEAT_TEXT, false)
                .date(FEAT_DTG, true)
                .point(FEAT_GEOM, true)
                .longType(FEAT_IN_REPLY_TO_USER_ID, false)
                .stringType(FEAT_IN_REPLY_TO_USER_NAME, false)
                .stringType(FEAT_IN_REPLY_TO_STATUS, false)
                .stringType(FEAT_HASHTAGS, false)
                .stringType(FEAT_USER_MENTIONS, false)
                .stringType(FEAT_URLS, false)
                .stringType(FEAT_SYMBOLS, false)
                .stringType(FEAT_MEDIA, false)
                .longType(FEAT_RETWEETS, false)
                .stringType(FEAT_IS_RETWEET, false)
                .stringType(FEAT_SOURCE, false)
                .stringType(FEAT_FILTER_LEVEL, false)
                .stringType(FEAT_WITHHELD_COPYRIGHT, false)
                .stringType(FEAT_WITHHELD_COUNTRIES, false)
                .stringType(FEAT_WITHHELD_SCOPE, false)
                .stringType(FEAT_LANGUAGE, false)
                .withIndexes(AvailableTables.Z3TableSchemeStr())
                .build(name);
    }

    final SimpleFeatureType twitterType;
    final boolean useExtendedFeatures;

    FeatureStore source;
    TwitterParser parser;

    String featureName;

    public TwitterFeatureIngester() {
        this("twitter", false);
    }

    public TwitterFeatureIngester(String featureName, boolean useExtendedFeatures) {
        this.twitterType = useExtendedFeatures ? buildExtended(featureName) : buildBasic(featureName);
        this.featureName = featureName;
        this.useExtendedFeatures = useExtendedFeatures;
    }

    public SimpleFeatureType getSchema() {
        return twitterType;
    }

    public void initialize(Map<String, Object> dataStoreParams) throws IOException {
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

        if(ds.getSchema(twitterType.getTypeName()) == null) {
            // schema doesn't exist, create it
            log.info("Creating Geomesa tables...");
            long startTime = System.currentTimeMillis();

            ds.createSchema(twitterType);

            long createTime = System.currentTimeMillis() - startTime;
            log.info("Created schema in " + createTime + "ms");
        } else {
            log.info("Geomesa tables exist...");
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
