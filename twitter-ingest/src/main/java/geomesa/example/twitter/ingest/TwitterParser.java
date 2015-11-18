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

import com.google.common.base.Joiner;
import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import org.apache.log4j.Logger;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static geomesa.example.twitter.ingest.TwitterFeatureIngester.*;

/**
 * Class to parse twitter data from an input stream that has one tweet as json per line
 */
public class TwitterParser {

    private static final Logger log = Logger.getLogger(TwitterParser.class);

    public static final String COORDS_GEO_JSON = "coordinates";
    public static final String COORDS = "coordinates";
    public static final String USER = "user";
    public static final String USER_ID = "id";
    public static final String USER_NAME = "name";
    public static final String TWEET_ID = "id";
    public static final String CREATED_AT = "created_at";
    public static final String TEXT = "text";

    public static final int BATCH_SIZE = 10_000;

    final GeometryFactory geoFac = new GeometryFactory();

    //Mon May 19 01:42:26 +0000 2014
    final DateTimeFormatter df = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy");

    final String featureName;
    final SimpleFeatureType twitterType;
    final boolean useExtendedFeatures;

    public TwitterParser(final String featureName, SimpleFeatureType sft, boolean useExtendedFeatures) {
        this.featureName = featureName;
        this.twitterType =  sft;
        this.useExtendedFeatures = useExtendedFeatures;
    }

    /**
     * Parse an input stream using GSON
     */
    public List<SimpleFeatureCollection> parse(final InputStream is, final String sourceName) throws IOException {
        log.info(sourceName + " - starting parsing");

        final long startTime = System.currentTimeMillis();

        final List<SimpleFeatureCollection> results = new ArrayList<>();
        final Reader bufReader = new BufferedReader(new InputStreamReader(is));
        final JsonReader jr = new JsonReader(bufReader);
        jr.setLenient(true);
        final JsonParser jp = new JsonParser();

        final SimpleFeatureBuilder builder = AvroSimpleFeatureFactory.featureBuilder(twitterType);
        builder.setValidating(true);

        DefaultFeatureCollection batch = new DefaultFeatureCollection(featureName, twitterType);

        long numGoodTweets = 0;
        long numBadTweets = 0;
        JsonObject obj;
        while ((obj = next(jp, jr, sourceName)) != null) {
            SimpleFeature sf = null;
            try {
                sf = convertToFeature(obj, builder, geoFac, df);
            } catch (Exception e) {
                // parsing error
            }
            if (sf != null && sf.getDefaultGeometry() != null) {
                batch.add(sf);
                numGoodTweets++;
                if (numGoodTweets % BATCH_SIZE == 0) {
                    results.add(batch);
                    batch = new DefaultFeatureCollection(featureName, twitterType);
                }
                if (numGoodTweets % 100_000 == 0) {
                    log.debug(Long.toString(numGoodTweets) + " records parsed");
                }
            } else {
                numBadTweets++;
            }
        }
        // add remaining results
        if (!batch.isEmpty()) {
            results.add(batch);
        }
        final long parseTime = System.currentTimeMillis() - startTime;
        log.info(sourceName + " - parsed " + numGoodTweets +" skipping " + numBadTweets +
                 " invalid tweets in " + parseTime + "ms");
        return results;
    }

    private SimpleFeature convertToFeature(final JsonObject obj,
                                           final SimpleFeatureBuilder builder,
                                           final GeometryFactory factory,
                                           final DateTimeFormatter df) {
        builder.reset();

        // user info
        final JsonObject user = obj.getAsJsonObject(USER);
        final String userName = user.getAsJsonPrimitive(USER_NAME).getAsString();
        long userId = user.getAsJsonPrimitive(USER_ID).getAsLong();
        builder.set(TwitterFeatureIngester.FEAT_USER_NAME, utf8(userName));
        builder.set(TwitterFeatureIngester.FEAT_USER_ID, userId);

        builder.set(TwitterFeatureIngester.FEAT_TEXT, utf8(obj.get(TEXT).getAsString()));

        // geo info
        final boolean hasGeoJson = obj.has(COORDS_GEO_JSON) && obj.get(COORDS_GEO_JSON) != JsonNull.INSTANCE;
        if (hasGeoJson) {
            final JsonObject geo = obj.getAsJsonObject(COORDS_GEO_JSON);
            final JsonArray coords = geo.getAsJsonArray(COORDS);
            double lat = coords.get(0).getAsDouble();
            double lon = coords.get(1).getAsDouble();

            if (lon !=  0.0 && lat != 0.0) {
                final Coordinate coordinate = new Coordinate(lat, lon);
                final Geometry g = new Point(new CoordinateArraySequence(new Coordinate[]{coordinate}), factory);
                builder.set(TwitterFeatureIngester.FEAT_GEOM, g);
            }
        }

        // time and id
        final long tweetId = obj.get(TWEET_ID).getAsLong();
        final Date date = df.parseDateTime(obj.get(CREATED_AT).getAsString()).toDate();
        builder.set(TwitterFeatureIngester.FEAT_TWEET_ID, tweetId);
        builder.set(TwitterFeatureIngester.FEAT_DTG, date);

        if (useExtendedFeatures) {
            conditionalSetString(builder, obj, FEAT_IS_RETWEET);
            conditionalSetString(builder, obj, FEAT_SOURCE);
            conditionalSetLong(builder, obj, FEAT_RETWEETS);
            conditionalSetLong(builder, obj, FEAT_IN_REPLY_TO_USER_ID);
            conditionalSetString(builder, obj, FEAT_IN_REPLY_TO_USER_NAME);
            conditionalSetString(builder, obj, FEAT_IN_REPLY_TO_STATUS);
            conditionalSetString(builder, obj, FEAT_FILTER_LEVEL);
            conditionalSetString(builder, obj, FEAT_LANGUAGE);
            conditionalSetString(builder, obj, FEAT_WITHHELD_COPYRIGHT);
            conditionalSetString(builder, obj, FEAT_WITHHELD_SCOPE);
            conditionalSetArray(builder, obj, FEAT_WITHHELD_COUNTRIES);

            JsonElement entities = obj.get("entities");
            if (entities != null && entities != JsonNull.INSTANCE) {
                JsonObject e = (JsonObject) entities;
                conditionalSetObjectArray(builder, e, FEAT_HASHTAGS, "text");
                conditionalSetObjectArray(builder, e, FEAT_URLS, "url");
                conditionalSetObjectArray(builder, e, FEAT_SYMBOLS, "text");
                conditionalSetObjectArray(builder, e, FEAT_USER_MENTIONS, "id");
                conditionalSetObjectArray(builder, e, FEAT_MEDIA, "media_url");
            }
        }

        return builder.buildFeature(Long.toString(tweetId));
    }

    private String utf8(String input) {
        return input == null ? null : input.replaceAll("\\p{C}", "?");
    }

    private void conditionalSetObjectArray(SimpleFeatureBuilder builder,
                                           JsonObject obj,
                                           String feature,
                                           String nestedAttribute) {

        JsonElement object = obj.get(feature);
        if (object != null && object != JsonNull.INSTANCE) {
            List<String> values = new ArrayList<>();
            for (Iterator<JsonElement> iter = ((JsonArray) object).iterator(); iter.hasNext();) {
                JsonElement next = iter.next();
                if (next != null && next != JsonNull.INSTANCE) {
                    JsonElement attribute = ((JsonObject) next).get(nestedAttribute);
                    if (attribute != null && attribute != JsonNull.INSTANCE) {
                        values.add(attribute.getAsString());
                    }
                }
            }
            if (!values.isEmpty()) {
                builder.set(feature, Joiner.on(",").join(values));
            }
        }
    }

    private void conditionalSetArray(SimpleFeatureBuilder builder, JsonObject obj, String feature) {
        JsonElement a = obj.get(feature);
        if (a != null && a != JsonNull.INSTANCE) {
            for (Iterator<JsonElement> i = ((JsonArray) a).iterator(); i.hasNext(); ) {
                JsonElement e = i.next();
                if (e != null && e != JsonNull.INSTANCE) {
                    builder.set(feature, e.getAsString());
                }
            }
        }
    }

    private void conditionalSetString(SimpleFeatureBuilder builder, JsonObject obj, String feature) {
        JsonElement e = obj.get(feature);
        if (e != null && e != JsonNull.INSTANCE) {
            builder.set(feature, e.getAsString());
        }
    }

    private void conditionalSetLong(SimpleFeatureBuilder builder, JsonObject obj, String feature) {
        JsonElement e = obj.get(feature);
        if (e != null && e != JsonNull.INSTANCE) {
            builder.set(feature, e.getAsLong());
        }
    }

    private JsonObject next(final JsonParser parser, final JsonReader reader, final String sourceName) throws IOException {
        while(reader.hasNext() && reader.peek() != JsonToken.END_DOCUMENT) {
            try {
                final JsonElement element = parser.parse(reader);
                if (element != null && element != JsonNull.INSTANCE) {
                    return element.getAsJsonObject();
                }
            } catch (Exception e) {
                log.error(sourceName + " - error parsing json", e);
                return null;
            }
        }
        return null;
    }

}

