package com.helium.ingestor.flows;

import static com.helium.ingestor.HeliumIngestorService.HELIUM_INGESTOR;
import static com.helium.ingestor.flows.CameraProcessRunnerFlow.readString;

import com.flower.anno.event.DisableEventProfiles;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.functions.StepFunction;
import com.flower.anno.functions.TransitFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.InOut;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.common.Output;
import com.flower.anno.params.transit.InRetOrException;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.InOutPrm;
import com.flower.conf.OutPrm;
import com.flower.conf.ReturnValueOrException;
import com.flower.conf.Transition;
import com.helium.ingestor.core.HeliumEventNotifier;
import com.helium.ingestor.core.HeliumEventType;
import com.helium.ingestor.flows.events.FlowTerminationEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: DRY With LoadVideoChannelsFlow, MergeChunkSubRangeFlow
@FlowType(firstStep = "LAUNCH_PROCESS")
@DisableEventProfiles({FlowTerminationEvent.class})
public class LoadVideoDurationFlow {
    final static Logger LOGGER = LoggerFactory.getLogger(LoadVideoDurationFlow.class);

    final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm_ss");
    final static String CMD = "ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 %s";
    final static Integer MAX_RETRIES = 3;

    @State final String cameraName;
    @State final String videoChunkFileName;
    @State final HeliumEventNotifier heliumEventNotifier;

    @State final StringBuilder stdoutOutput;
    @State final StringBuilder stderrOutput;
    @State final String command;
    @State @Nullable Process process;
    @State @Nullable BufferedReader stdout;
    @State @Nullable BufferedReader stderr;
    @State int attempt = 1;

    @State @Nullable Double durationSeconds;
    @State @Nullable Throwable durationException;

    public LoadVideoDurationFlow(String cameraName, File videoChunkFile, HeliumEventNotifier heliumEventNotifier) {
        this.cameraName = cameraName;
        this.videoChunkFileName = videoChunkFile.getAbsolutePath();
        this.command = String.format(CMD, videoChunkFileName);
        this.heliumEventNotifier = heliumEventNotifier;
        this.stdoutOutput = new StringBuilder();
        this.stderrOutput = new StringBuilder();
    }

    @StepFunction(transit = "LAUNCH_PROCESS_TRANSITION")
    public static void LAUNCH_PROCESS(
            @In String command,
            @In String videoChunkFileName,
            @Out OutPrm<Process> process,
            @Out OutPrm<BufferedReader> stdout,
            @Out OutPrm<BufferedReader> stderr
    ) throws IOException {
        LOGGER.debug("Getting video chunk duration {}, cmd: {}", videoChunkFileName, command);

        Process newProcess = Runtime.getRuntime().exec(command);

        stdout.setOutValue(new BufferedReader(new InputStreamReader(newProcess.getInputStream())));
        stderr.setOutValue(new BufferedReader(new InputStreamReader(newProcess.getErrorStream())));
        process.setOutValue(newProcess);
    }

    @TransitFunction
    public static Transition LAUNCH_PROCESS_TRANSITION(
            @In String cameraName,
            @In String command,
            @Out(out = Output.OPTIONAL) OutPrm<Throwable> durationException,
            @InOut(throwIfNull = true, out = Output.OPTIONAL) InOutPrm<Integer> attempt,
            @InRetOrException ReturnValueOrException<Void> retValOrExc,
            @StepRef Transition LAUNCH_PROCESS,
            @StepRef Transition READ_PROCESS_OUTPUT,
            @Terminal Transition END
    ) {
        if (retValOrExc.exception().isPresent()) {
            int currentAttempt = attempt.getInValue();
            LOGGER.error("Error getting video chunk duration: attempt [{}] camera [{}] process [{}]", currentAttempt,
                    cameraName, command, retValOrExc.exception().get());

            if (currentAttempt < MAX_RETRIES) {
                int delay = 500;//ms
                attempt.setOutValue(currentAttempt + 1);
                return LAUNCH_PROCESS.setDelay(Duration.ofMillis(delay));
            } else {
                durationException.setOutValue(retValOrExc.exception().get());
                return END;
            }
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
            @Out(out = Output.OPTIONAL) OutPrm<Double> durationSeconds,
            @Out(out = Output.OPTIONAL) OutPrm<Throwable> durationException,

            @Terminal Transition END
    ) {
        if (process.exitValue() == 0) {
            try {
                String durationSecondsStr = stdoutOutput.toString().trim();
                Double durationSecondsVal = Double.parseDouble(durationSecondsStr);

                durationSeconds.setOutValue(durationSecondsVal);
            } catch (Exception e) {
                durationException.setOutValue(e);
            }
        } else {
            String stderr = stderrOutput.toString();
            durationException.setOutValue(new Exception(stderr));
        }
        return END;
    }
}
