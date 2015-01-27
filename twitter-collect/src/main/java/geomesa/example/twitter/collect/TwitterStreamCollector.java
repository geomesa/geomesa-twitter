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

package geomesa.example.twitter.collect;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreamCollector {

    private static final Logger log = Logger.getLogger(TwitterStreamCollector.class);

    static int QUEUE_SIZE = 50000;
    static int MAX_TWEETS_PER_FILE = 10000;

    // Set a bbox on a spot of interest for twitter analysis
//    static double LAT_MIN = 38.72009;
//    static double LAT_MAX = 38.77019;
//    static double LON_MIN = -90.32023;
//    static double LON_MAX = -90.23957;

    static double LAT_MIN = 38.6;
    static double LAT_MAX = 38.8;
    static double LON_MIN = -76.8;
    static double LON_MAX = -77.2;

    static final Location.Coordinate SOUTHWEST_CORNER = new Location.Coordinate(LON_MIN, LAT_MIN);
    static final Location.Coordinate NORTHEAST_CORNER = new Location.Coordinate(LON_MAX, LAT_MAX);

    static final Location COLLECTION_LOCATION = new Location(SOUTHWEST_CORNER, NORTHEAST_CORNER);

    private final File outputDir;
    private final String consumerKey;
    private final String consumerSecret;
    private final String token;
    private final String secret;

    public TwitterStreamCollector(File outputDir, String consumerKey, String consumerSecret, String token, String secret) {
        this.outputDir = outputDir;
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.token = token;
        this.secret = secret;
    }

    public void collect() throws IOException {

        final BlockingQueue<String> queue = new LinkedBlockingQueue<>(QUEUE_SIZE);
        final StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        // Set location to collect on
        endpoint.locations(Lists.newArrayList(COLLECTION_LOCATION));

        // add some track terms with this code:
        // endpoint.trackTerms(Lists.newArrayList("twitterapi", "#yolo"));

        final Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

        // Create a new BasicClient. By default gzip is enabled.
        final Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();

        while (!client.isDone()) {
            writeTweetsToFile(getFileToWrite(), queue);
        }

        client.stop();
        log.info("Client stopped, restart needed");
    }

    public void collect2() throws IOException {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.locations(Lists.newArrayList(COLLECTION_LOCATION));

        // auth
        final Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .hosts(hosebirdHosts)
                .authentication(auth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        // Attempts to establish a connection.
        hosebirdClient.connect();

        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            writeTweetsToFile(getFileToWrite(), msgQueue);
        }

        hosebirdClient.stop();
    }

    private void writeTweetsToFile(File file, final BlockingQueue<String> queue) throws IOException {
        log.info("Writing tweets to " + file.getName());
        final BufferedWriter bw = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));
        try {
            for (int msgRead = 0; msgRead < MAX_TWEETS_PER_FILE; msgRead++) {
                try {
                    final String msg = queue.take();
                    bw.write(msg);
                }
                catch (InterruptedException e){
                    throw new IOException("Error taking tweets off queue", e);
                }
            }
        }
        finally{
            bw.close();
        }
    }

    private File getOrCreateDayDir(final Date date){
        final DateFormat df = new SimpleDateFormat("yyyyMMdd");

        final File dayDir = new File(this.outputDir, df.format(date));
        if (!dayDir.exists()) {
            dayDir.mkdir();
        }

        return dayDir;
    }

    private File getFileToWrite() throws IOException {
        final Date now = new Date();
        final File dayDir = getOrCreateDayDir(now);

        final DateFormat dfd = new SimpleDateFormat("yyyyMMdd-HHmmss");
        final File file = new File(dayDir, dfd.format(now) + ".txt");
        file.createNewFile();
        return file;
    }

}