package com.helium.ingestor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.helium.ingestor.config.Config;
import java.io.File;

import com.helium.ingestor.videoservice.HttpStaticFileServer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Main {
    static final String CONFIG_OPTION_NAME = "config";
    static final String HELIUM_INGESTOR_APP_NAME = "HeliumIngestor";

    public static void main(String[] args) {
        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption("c", CONFIG_OPTION_NAME, true, "Config file name");

        try {
            // Parse the command line arguments
            CommandLine line = parser.parse(options, args);

            if (line.hasOption(CONFIG_OPTION_NAME)) {
                String configName = line.getOptionValue(CONFIG_OPTION_NAME);
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
                        .registerModule(new GuavaModule());

                Config config;
                File configFile = new File(configName);
                if (configFile.exists()) {
                    config = mapper.readValue(configFile, Config.class);
                } else {
                    String resourceStr = Resources.toString(Resources.getResource(configName), Charsets.UTF_8);
                    config = mapper.readValue(resourceStr, Config.class);
                }

                File videoFolder = new File(config.videoFeedFolder());
                if (!videoFolder.exists()) {
                    videoFolder.mkdirs();
                }

                HttpStaticFileServer videoServer = null;
                if (config.videoService() != null) {
                    videoServer = new HttpStaticFileServer();
                    videoServer.startServer(videoFolder, false, config.videoService().port());
                }

                //This will block
                HeliumIngestorService.run(config);

                if (videoServer != null) {
                    videoServer.stopServer();
                }
            } else {
                System.out.println("Please specify config name");

                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(HELIUM_INGESTOR_APP_NAME, options);
            }
        } catch (ParseException pe) {
            System.out.println("Argument parsing error: " + pe.getMessage());

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(HELIUM_INGESTOR_APP_NAME, options);
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
