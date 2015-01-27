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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;

public class Runner {
    private static final Logger log = Logger.getLogger(Runner.class);

    public static void main(String[] args) throws Exception {
        // configure log4j
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        // parse args
        final MyArgs clArgs = new MyArgs();
        final JCommander jc = new JCommander(clArgs);
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            log.info("Error parsing arguments: " + e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        final File outputDir = new File(clArgs.outputDir);
        if(outputDir.exists()) {
           if(!outputDir.isDirectory()){
               log.error("Output Directory "+outputDir.getPath() + " is not a directory");
               System.exit(-1);
           }
        }
        else {
            outputDir.mkdirs();
        }

        // collect tweets
        final TwitterStreamCollector collector = new TwitterStreamCollector(
                outputDir,
                clArgs.consumerKey,
                clArgs.consumerSecret,
                clArgs.token,
                clArgs.secret);

        collector.collect2();
    }

    static class MyArgs {
        @Parameter(names = {"--consumerKey", "-ck"}, description = "Twitter consumer key", required = true)
        String consumerKey;

        @Parameter(names = {"--consumerSecret", "-cs"}, description = "Twitter consumer secret", required = true)
        String consumerSecret;

        @Parameter(names = {"--token", "-t"}, description = "Twitter token", required = true)
        String token;

        @Parameter(names = {"--secret", "-s"}, description = "Twitter secret", required = true)
        String secret;

        @Parameter(names = {"--outputDir", "-o"}, description = "Output Directory", required = true)
        String outputDir;
    }
}
