package com.helium.ingestor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.helium.ingestor.config.Config;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;

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
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
                        .registerModule(new GuavaModule());

                File configFile = new File(line.getOptionValue(CONFIG_OPTION_NAME));
                Config config = mapper.readValue(configFile, Config.class);

                HeliumService.run(config);
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
