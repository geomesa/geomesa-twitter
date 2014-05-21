package geomesa.example.twitter.ingest;

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
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.util.Date;

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

    final GeometryFactory geoFac = new GeometryFactory();

    //Mon May 19 01:42:26 +0000 2014
    final DateTimeFormatter df = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy");

    final String featureName;
    final SimpleFeatureType twitterType;

    public TwitterParser(final String featureName, SimpleFeatureType sft) {
        this.featureName = featureName;
        this.twitterType =  sft;
    }

    /**
     * Parse an input stream using GSON
     */
    public SimpleFeatureCollection parse(final InputStream is, final String sourceName) throws IOException {
        log.info("Starting parse of " + sourceName);

        final DefaultFeatureCollection results = new DefaultFeatureCollection(featureName, twitterType);
        final Reader bufReader = new BufferedReader(new InputStreamReader(is));
        final JsonReader jr = new JsonReader(bufReader);
        jr.setLenient(true);
        final JsonParser jp = new JsonParser();

        final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(twitterType);
        builder.setValidating(true);

        long numGoodTweets = 0;
        long noGeo = 0;
        JsonObject obj;
        while( (obj = next(jp, jr, sourceName)) != null) {
            final SimpleFeature sf = convertToFeature(obj, builder, geoFac, df);
            if(sf.getDefaultGeometry() != null){
                results.add(sf);
                numGoodTweets += 1;
                if(numGoodTweets % 10_000 == 0){
                    log.info(Long.toString(numGoodTweets) + " records parsed");
                }
            }
            else {
                noGeo++;
            }
        }

        log.info("Parsed "+Long.toString(numGoodTweets) +" skipping "+ Long.toString(noGeo) +" with no geo");
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
        builder.set(TwitterFeatureIngester.FEAT_USER_NAME, userName);
        builder.set(TwitterFeatureIngester.FEAT_USER_ID, userId);

        final String text = obj.get(TEXT).getAsString();
        builder.set(TwitterFeatureIngester.FEAT_TEXT, text);

        // geo info
        final boolean hasGeoJson = obj.has(COORDS_GEO_JSON) && obj.get(COORDS_GEO_JSON) != JsonNull.INSTANCE;
        if (hasGeoJson) {
            final JsonObject geo = obj.getAsJsonObject(COORDS_GEO_JSON);
            final JsonArray coords = geo.getAsJsonArray(COORDS);
            double lat = coords.get(0).getAsDouble();
            double lon = coords.get(1).getAsDouble();

            if(lon !=  0.0 && lat != 0.0) {
                final Coordinate coordinate = new Coordinate(lat, lon);
                final Geometry g = new Point(new CoordinateArraySequence(new Coordinate[]{coordinate}), factory);
                builder.set(TwitterFeatureIngester.FEAT_GEOM, g);
            }
        }

        // time and id
        final long tweetId = obj.get(TWEET_ID).getAsLong();
        final Date date = df.parseDateTime(obj.get(CREATED_AT).getAsString()).toDate();
        builder.set(TwitterFeatureIngester.FEAT_TWEET_ID, tweetId);
        builder.set(TwitterFeatureIngester.FEAT_DTG,date);

        return builder.buildFeature(Long.toString(tweetId));
    }

    private JsonObject next(final JsonParser parser, final JsonReader reader, final String sourceName) throws IOException {
        while(reader.hasNext() && reader.peek() != JsonToken.END_DOCUMENT) {
            try {
                final JsonElement element = parser.parse(reader);
                if (element != null && element != JsonNull.INSTANCE) {
                    return element.getAsJsonObject();
                }
            } catch (Exception e) {
                log.error("Error parsing json from source " + sourceName, e);
                return null;
            }
        }
        return null;
    }

}

