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
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

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

        final TwitterStreamCollector collector = new TwitterStreamCollector(
                outputDir,
                clArgs.consumerKey,
                clArgs.consumerSecret,
                clArgs.token,
                clArgs.secret);

        collector.collect();
    }

    static class MyArgs {
        @Parameter(names = {"--consumerKey"}, description = "Twitter consumer key", required = true)
        String consumerKey;

        @Parameter(names = {"--consumerSecret"}, description = "Twitter consumer secret", required = true)
        String consumerSecret;

        @Parameter(names = {"--token"}, description = "Twitter token", required = true)
        String token;

        @Parameter(names = {"--secret"}, description = "Twitter secret", required = true)
        String secret;

        @Parameter(names = {"--outputDir"}, description = "Output Directory", required = true)
        String outputDir;
    }
}
