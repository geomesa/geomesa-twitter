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
 * java -jar twitter-ingest-1.0-SNAPSHOT.jar --instanceId instance --user root --password secret --zookeepers 'zoo1,zoo2,zoo3' --catalog twitter_tutorial  --featureName twitter_tutorial file1.txt file2.txt file3.txt
 */
public class Runner {

    private static final Logger log = Logger.getLogger(Runner.class);

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        Logger.getLogger("geomesa.example").setLevel(Level.DEBUG);

        //parse args
        final IngestArgs clArgs = new IngestArgs();
        final JCommander jc = new JCommander(clArgs);
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            log.info("Error parsing arguments: " + e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        final TwitterFeatureIngester ingester = new TwitterFeatureIngester(clArgs.featureName, Boolean.valueOf(clArgs.extendedFeatures));

        final Map<String, Object> params = new HashMap<>();
        params.put("instanceId", clArgs.instanceId);
        params.put("zookeepers", clArgs.zookeepers);
        params.put("user", clArgs.user);
        params.put("password", clArgs.password);
        params.put("tableName", clArgs.catalog);
        params.put("indexSchemaFormat", clArgs.indexSchemaFormat);

        ingester.initialize(params);

        log.info("Beginning ingest...");

        for(final String fileName: clArgs.files) {
            final File file = new File(fileName);
            ingester.ingest(file);
        }

        log.info("Ingest completed");
    }

    public static class IngestArgs extends GeoMesaFeatureArgs {
        @Parameter(description = "files", required = true)
        public List<String> files = new ArrayList<>();

        @Parameter(names= {"--useExtendedFeatures", "-e"}, description = "parse extended features or the minimal set", required = false)
        public String extendedFeatures;

        @Parameter(names= {"--shards", "-s"}, description = "number of shards to use for data", required = false)
        public String shards;
    }

    public static class GeoMesaFeatureArgs extends GeomesaArgs {
        @Parameter(names= {"--featureName", "-f"}, description = "featureName to assign to the data", required = true)
        public String featureName;
    }

    public static class GeomesaArgs{
        @Parameter(names = {"--instanceId", "-i"}, description = "Name of the Accumulo Instance", required = true)
        public String instanceId;

        @Parameter(names = {"--zookeepers", "-z"}, description = "Comma separated list of zookeepers", required = true)
        public String zookeepers;

        @Parameter(names = {"--user", "-u"}, description = "Accumulo user name", required = true)
        public String user;

        @Parameter(names = {"--password", "-p"}, description = "Accumulo password", required = true)
        public String password;

        @Parameter(names = {"--catalog", "-c"}, description = "Accumulo catalog table name", required = true)
        public String catalog;

        @Parameter(names = {"--indexSchemaFormat"}, description = "Schema for indexing data", required = false)
        public String indexSchemaFormat;
    }
}
