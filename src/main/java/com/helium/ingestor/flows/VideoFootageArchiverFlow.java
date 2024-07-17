package com.helium.ingestor.flows;

import static com.helium.ingestor.flows.CameraProcessRunnerFlow.readString;

import com.flower.anno.event.DisableEventProfiles;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.functions.StepFunction;
import com.flower.anno.functions.TransitFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.transit.InRetOrException;
import com.flower.anno.params.transit.StepRef;
import com.flower.conf.OutPrm;
import com.flower.conf.ReturnValueOrException;
import com.flower.conf.Transition;
import com.helium.ingestor.config.Config;
import com.helium.ingestor.core.HeliumEventNotifier;
import com.helium.ingestor.flows.events.FlowTerminationEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: finish implementation
@FlowType(firstStep = "LAUNCH_PROCESS")
@DisableEventProfiles({FlowTerminationEvent.class})
public class VideoFootageArchiverFlow {
    final static Logger LOGGER = LoggerFactory.getLogger(VideoFootageArchiverFlow.class);
    final static int MAX_USED_PERCENT_BEFORE_ARCHIVING = 60;
    final static int MAX_USED_PERCENT_BEFORE_DELETING = 80;

    static final String CMD = "df -h %s";
    final static Integer MAX_RETRIES = 3;

    @State final File outputFolder;
    @State final Config config;
    @State final HeliumEventNotifier heliumEventNotifier;

    @State final StringBuilder stdoutOutput;
    @State final StringBuilder stderrOutput;
    @State final String command;

    @State @Nullable Process process;
    @State @Nullable BufferedReader stdout;
    @State @Nullable BufferedReader stderr;

    public VideoFootageArchiverFlow(Config config, HeliumEventNotifier heliumEventNotifier) {
        this.outputFolder = new File(config.videoFeedFolder());
        this.config = config;
        this.command = String.format(CMD, outputFolder.getAbsolutePath());
        this.heliumEventNotifier = heliumEventNotifier;
        this.stdoutOutput = new StringBuilder();
        this.stderrOutput = new StringBuilder();
    }

    @StepFunction(transit = "LAUNCH_PROCESS_TRANSITION")
    public static void LAUNCH_PROCESS(
            @In String command,

            @Out OutPrm<Process> process,
            @Out OutPrm<BufferedReader> stdout,
            @Out OutPrm<BufferedReader> stderr
    ) throws IOException {
        LOGGER.info("Getting free disk space {}", command);

        Process newProcess = Runtime.getRuntime().exec(command);

        stdout.setOutValue(new BufferedReader(new InputStreamReader(newProcess.getInputStream())));
        stderr.setOutValue(new BufferedReader(new InputStreamReader(newProcess.getErrorStream())));
        process.setOutValue(newProcess);
    }

    @TransitFunction
    public static Transition LAUNCH_PROCESS_TRANSITION(
            @InRetOrException ReturnValueOrException<Void> retValOrExc,
            @In String command,
            @StepRef Transition LAUNCH_PROCESS,
            @StepRef Transition READ_PROCESS_OUTPUT
    ) {
        if (retValOrExc.exception().isPresent()) {
            LOGGER.error("Error getting free disk space: command [{}]",
                    command, retValOrExc.exception().get());

            //TODO: report event?
            return LAUNCH_PROCESS.setDelay(Duration.ofMinutes(1));
        } else {
            return READ_PROCESS_OUTPUT;
        }
    }

    @SimpleStepFunction
    public static Transition READ_PROCESS_OUTPUT(
            @In(throwIfNull = true) Process process,
            @In(throwIfNull = true) BufferedReader stdout,
            @In(throwIfNull = true) BufferedReader stderr,

            @In StringBuilder stdoutOutput,
            @In StringBuilder stderrOutput,

            @StepRef Transition READ_PROCESS_OUTPUT,
            @StepRef Transition PARSE_OUTPUT
    ) throws IOException {
        stdoutOutput.append(readString(stdout));
        stderrOutput.append(readString(stderr));

        if (process.isAlive()) {
            return READ_PROCESS_OUTPUT.setDelay(Duration.ofMillis(100));
        } else {
            return PARSE_OUTPUT.setDelay(Duration.ofMillis(100));
        }
    }

    @SimpleStepFunction
    public static Transition PARSE_OUTPUT(
            @In(throwIfNull = true) Process process,
            @In StringBuilder stdoutOutput,
            @In StringBuilder stderrOutput,

            @StepRef Transition LAUNCH_PROCESS,
            @StepRef Transition TRY_ARCHIVE_FILE_AND_DELETE_IF_FAILS,
            @StepRef Transition TRY_ARCHIVE_FILE_AND_RETRY_IF_FAILS
    ) {
        if (process.exitValue() == 0) {
            try {
                Double percentUsed = getPercentUsed(stdoutOutput);
                if (percentUsed == null) {
                    throw new RuntimeException("Can't find used disk percentage");
                }

                if (percentUsed >= MAX_USED_PERCENT_BEFORE_DELETING) {
                    //Try to archive and if archivation fails, delete
                    return TRY_ARCHIVE_FILE_AND_DELETE_IF_FAILS;
                } else if (percentUsed >= MAX_USED_PERCENT_BEFORE_ARCHIVING) {
                    //Try to archive and retry archiving if archivation fails, do not delete
                    return TRY_ARCHIVE_FILE_AND_RETRY_IF_FAILS;
                } else {
                    return LAUNCH_PROCESS.setDelay(Duration.ofMinutes(1));
                }
            } catch (Exception e) {
                LOGGER.error("Error parsing `df` output:\nstdout {}\nstderr {}", stdoutOutput, stderrOutput, e);
                return LAUNCH_PROCESS.setDelay(Duration.ofMinutes(1));
            }
        } else {
            LOGGER.error("Error parsing `df` output:\nstdout {}\nstderr {}", stdoutOutput, stderrOutput);
            return LAUNCH_PROCESS.setDelay(Duration.ofMinutes(1));
        }
    }

    @SimpleStepFunction
    public static Transition TRY_ARCHIVE_FILE_AND_DELETE_IF_FAILS(
        @StepRef Transition LAUNCH_PROCESS
    ) {
        return LAUNCH_PROCESS;
    }

    @SimpleStepFunction
    public static Transition TRY_ARCHIVE_FILE_AND_RETRY_IF_FAILS(
            @StepRef Transition LAUNCH_PROCESS
    ) {
        return LAUNCH_PROCESS;
    }

    // ---------------------------------------------------------------------------------

    @Nullable static Double getPercentUsed(StringBuilder stdoutOutput) {
        String dfOutput = stdoutOutput.toString().trim();
        String[] lines = dfOutput.split("\n");

        Double percentUsed = null;
        for (String line : lines) {
            String[] words = line.split("\\s+");
            for (String word : words) {
                if (word.endsWith("%")) {
                    try {
                        percentUsed = Double.parseDouble(word.substring(0, word.length()-1));
                        break;
                    } catch (Exception e) {
                        //do nothing, it could've been "Use%"
                    }
                }
            }
            if (percentUsed != null) { break; }
        }

        return percentUsed;
    }
}
