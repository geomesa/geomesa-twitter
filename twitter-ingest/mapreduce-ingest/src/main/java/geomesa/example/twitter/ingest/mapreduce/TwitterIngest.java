package geomesa.example.twitter.ingest.mapreduce;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Joiner;
import geomesa.example.twitter.ingest.Runner;
import geomesa.example.twitter.ingest.TwitterFeatureIngester;
import geomesa.example.twitter.ingest.TwitterParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Example map reduce job to insert data into a geomesa data store from hdfs where the
 * tweets are stored as 1 tweet per line as json (possibly gzipped)
 */
public class TwitterIngest extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(TwitterIngest.class);

    private static final String PRE = "geomesa.example.ingest.";
    public static final String USER = PRE + "user";
    public static final String PASS = PRE + "pass";
    public static final String CATALOG = PRE + "catalog";
    public static final String INSTANCE = PRE + "instance";
    public static final String ZOOKEEPERS = PRE + "zookeepers";
    public static final String FEATURE = PRE + "featurename";
    public static final String SFT = PRE + "sft";
    public static final String EXTENDED_FEATURES = PRE + "extendedFeatures";
    public static final String PARSE_ONLY = PRE + "parseonly";

    @Override
    public int run(String[] args) throws Exception {
        // Parse command line args
        final TwitterIngestArgs jca = new TwitterIngestArgs();
        final JCommander jc = new JCommander(jca);
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            logger.error("Error parsing args: " + e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        // Test the sft to make sure it works
        final SimpleFeatureType sft = Boolean.valueOf(jca.extendedFeatures)
                ? TwitterFeatureIngester.buildExtended(jca.featureName)
                : TwitterFeatureIngester.buildBasic(jca.featureName);

        // Create the datastore
        final Map<String, Object> dataStoreParams = new HashMap<>();
        dataStoreParams.put("instanceId", jca.instanceId);
        dataStoreParams.put("zookeepers", jca.zookeepers);
        dataStoreParams.put("user", jca.user);
        dataStoreParams.put("password", jca.password);
        dataStoreParams.put("tableName", jca.catalog);

        final AccumuloDataStore ds;
        try {
            ds = (AccumuloDataStore)DataStoreFinder.getDataStore(dataStoreParams);
            if (ds == null) {
                throw new IOException("No data store returned");
            }
        } catch (IOException e) {
            throw new IOException("Error creating Geomesa datastore ", e);
        }

        if (ds.getSchema(sft.getTypeName()) == null) {
            // schema doesn't exist, create it
            logger.info("Creating Geomesa tables...");
            long startTime = System.currentTimeMillis();

            if (jca.shards != null) {
                int shards = Integer.parseInt(jca.shards);
                ds.createSchema(sft);
                logger.info("Created schema with custom max shards: " + Integer.toString(shards -1));
            } else {
                ds.createSchema(sft);
            }

            long createTime = System.currentTimeMillis() - startTime;
            logger.info("Created schema in " + createTime + "ms");
        } else {
            logger.info("Geomesa tables exist...continuing with mapreduce job");
        }

        // Set up the info needed in the mappers
        final Configuration conf = getConf();
        conf.set(USER, jca.user);
        conf.set(PASS, jca.password);
        conf.set(CATALOG, jca.catalog);
        conf.set(INSTANCE, jca.instanceId);
        conf.set(ZOOKEEPERS, jca.zookeepers);
        conf.set(FEATURE, jca.featureName);
        conf.set(SFT, SimpleFeatureTypes.encodeType(sft));
        conf.set(EXTENDED_FEATURES, Boolean.valueOf(jca.extendedFeatures).toString());
        conf.set(PARSE_ONLY, Boolean.valueOf(jca.parseOnly).toString());

        // Create the job and set input/output formats and input files
        final Job job = Job.getInstance(getConf());
        job.setJarByClass(TwitterMapper.class);

        job.setMapperClass(TwitterMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapSpeculativeExecution(false);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job, Joiner.on(",").join(jca.files));

        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);

        final long startTime = System.currentTimeMillis();
        logger.info("Submitting Twitter Ingest Job");
        final int res = job.waitForCompletion(true) ? 0 : 1;
        final long endTime = System.currentTimeMillis();

        final long count = job.getCounters().findCounter(TwitterMapper.COUNTERS.FEATURES_WRITTEN).getValue();
        final double rate = (double)count / ((double) ((endTime-startTime) / 1000) );
        logger.info(String.format("Ingest rate: %.2f features/sec", rate));

        return res;
    }

    public static class TwitterMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

        private TwitterParser parser;
        private FeatureWriter<SimpleFeatureType, SimpleFeature> featureWriter;
        boolean debugged = false;
        boolean parseOnly = false;

        public enum COUNTERS {
            PARSE_SUCCESS,
            FEATURES_WRITTEN,
            PARSE_FAIL
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            final Configuration conf = context.getConfiguration();

            // set up the geomesa writer and parser
            final SimpleFeatureType sft = SimpleFeatureTypes.createType(conf.get(FEATURE), conf.get(SFT));
            parser = new TwitterParser(conf.get(FEATURE), sft, Boolean.valueOf(conf.get(EXTENDED_FEATURES)));

            final Map<String, Object> dataStoreParams = new HashMap<>();
            dataStoreParams.put("instanceId", conf.get(INSTANCE));
            dataStoreParams.put("zookeepers", conf.get(ZOOKEEPERS));
            dataStoreParams.put("user", conf.get(USER));
            dataStoreParams.put("password", conf.get(PASS));
            dataStoreParams.put("tableName", conf.get(CATALOG));

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
                this.featureWriter = ds.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT);
            } catch (IOException e) {
                throw new IOException("Unable to create feature writer", e);
            }

            parseOnly = Boolean.valueOf(conf.get(PARSE_ONLY));
            if (parseOnly) logger.info("Running in parse only mode");

            logger.info("Mapper Initialization complete");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (parseOnly){
                fakeMap(key, value, context);
            } else {
                realMap(key, value, context);
            }
        }

        protected void realMap(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final SimpleFeature toWrite = featureWriter.next();
            final boolean success = parser.parse(value.toString(), toWrite);
            if (success) {
                context.getCounter(COUNTERS.PARSE_SUCCESS).increment(1);
                featureWriter.write();
                context.getCounter(COUNTERS.FEATURES_WRITTEN).increment(1);
            } else {
                if (!debugged) {
                    logger.info("bad: " + value.toString());
                    debugged = true;
                }
                context.getCounter(COUNTERS.PARSE_FAIL).increment(1);
            }
        }

        protected void fakeMap(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            SimpleFeature sf = parser.parse(value.toString());
            if (sf != null) {
                context.getCounter(COUNTERS.PARSE_SUCCESS).increment(1);
                context.getCounter(COUNTERS.FEATURES_WRITTEN).increment(1);
            } else {
                if (!debugged) {
                    logger.info("bad: " + value.toString());
                    debugged = true;
                }
                context.getCounter(COUNTERS.PARSE_FAIL).increment(1);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            featureWriter.close();
        }
    }

    // Place holder to tune parameters for ingest later
    public static class TwitterIngestArgs extends Runner.IngestArgs {
        @Parameter(names= {"--parse-only"}, description = "fake writing data and just parse it...", required = false)
        public boolean parseOnly = false;
    }

    public static void main(String[] args) throws Exception {
        final int res = ToolRunner.run(new TwitterIngest(), args);
        System.exit(res);
    }
}
