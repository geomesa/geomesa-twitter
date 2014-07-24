package geomesa.example.twitter.ingest;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Run the ingest...should be a shaded jar installed to ~/.m2/repository
 *
 * Last arg is files and you can use the * syntax in bash shell to pass multiple files
 *
 * java -jar twitter-ingest-1.0-SNAPSHOT.jar --instanceId instance --user root --password secret --zookeepers 'zoo1,zoo2,zoo3' --tableName twitter_tutorial  --featureName twitter_tutorial file1.txt file2.txt file3.txt
 */
public class Runner {

    private static final Logger log = Logger.getLogger(Runner.class);

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        Logger.getLogger("geomesa.example").setLevel(Level.DEBUG);

        //parse args
        final MyArgs clArgs = new MyArgs();
        final JCommander jc = new JCommander(clArgs);
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            log.info("Error parsing arguments: " + e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        final TwitterFeatureIngester ingester = new TwitterFeatureIngester(Boolean.valueOf(clArgs.extendedFeatures));

        final Map<String, Object> params = new HashMap<>();
        params.put("instanceId", clArgs.instanceId);
        params.put("zookeepers", clArgs.zookeepers);
        params.put("user", clArgs.user);
        params.put("password", clArgs.password);
        params.put("tableName", clArgs.tableName);
        params.put("indexSchemaFormat", clArgs.indexSchemaFormat);

        ingester.initialize(clArgs.featureName, params);

        log.info("Beginning ingest...");

        for(final String fileName: clArgs.files) {
            final File file = new File(fileName);
            ingester.ingest(file);
        }

        log.info("Ingest completed");
    }

    static class MyArgs extends GeomesaArgs {
        @Parameter(description = "files", required = true)
        List<String> files = new ArrayList<>();

        @Parameter(names= {"--featureName"}, description = "featureName to assign to the data", required = true)
        String featureName;

        @Parameter(names= {"--useExtendedFeatures"}, description = "parse extended features or the minimal set", required = false)
        String extendedFeatures;

        @Parameter(names= {"--shards"}, description = "number of shards to use for data", required = false)
        String shards;
    }

    public static class GeomesaArgs{
        @Parameter(names = {"--instanceId"}, description = "Name of the Accumulo Instance", required = true)
        String instanceId;

        @Parameter(names = {"--zookeepers"}, description = "Comma separated list of zookeepers", required = true)
        String zookeepers;

        @Parameter(names = {"--user"}, description = "Accumulo user name", required = true)
        String user;

        @Parameter(names = {"--password"}, description = "Accumulo password", required = true)
        String password;

        @Parameter(names = {"--tableName"}, description = "Accumulo table name", required = true)
        String tableName;

        @Parameter(names = {"--indexSchemaFormat"}, description = "Schema for indexing data", required = false)
        String indexSchemaFormat;
    }
}
