package com.helium.ingestor.flows;

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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.helium.ingestor.core.HeliumEventNotifier;
import com.helium.ingestor.core.HeliumEventType;
import com.helium.ingestor.flows.events.FlowTerminationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.helium.ingestor.flows.CameraProcessRunnerFlow.readString;

@FlowType(firstStep = "LAUNCH_PROCESS")
@DisableEventProfiles({FlowTerminationEvent.class})
public class LoadChunkDurationFlow {
    final static Logger LOGGER = LoggerFactory.getLogger(LoadChunkDurationFlow.class);

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

    public LoadChunkDurationFlow(String cameraName, String videoChunkFileName, HeliumEventNotifier heliumEventNotifier) {
        this.cameraName = cameraName;
        this.videoChunkFileName = videoChunkFileName;
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
        //LOGGER.debug("Getting chunk duration {}, cmd: {}", videoChunkFileName, command);
        LOGGER.info("Getting chunk duration {}, cmd: {}", videoChunkFileName, command);

        Process newProcess = Runtime.getRuntime().exec(command);

        stdout.setOutValue(new BufferedReader(new InputStreamReader(newProcess.getInputStream())));
        stderr.setOutValue(new BufferedReader(new InputStreamReader(newProcess.getErrorStream())));
        process.setOutValue(newProcess);
    }

    @TransitFunction
    public static Transition LAUNCH_PROCESS_TRANSITION(
            @In String cameraName,
            @In String command,
            @In String videoChunkFileName,
            @In HeliumEventNotifier heliumEventNotifier,
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
                sendFailedToReadVideoChunkEvent(currentAttempt, cameraName, command, videoChunkFileName, heliumEventNotifier, -1, "N/A", "N/A");
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
    public static ListenableFuture<Transition> PARSE_OUTPUT(
            @In(throwIfNull = true) Process process,
            @In StringBuilder stdoutOutput,
            @In StringBuilder stderrOutput,
            @Out OutPrm<Double> durationSeconds,

            @In int attempt,
            @In String cameraName,
            @In String command,
            @In String videoChunkFileName,
            @In HeliumEventNotifier heliumEventNotifier,

            @Terminal Transition END
    ) {
        if (process.exitValue() == 0) {
            try {
                String durationSecondsStr = stdoutOutput.toString().trim();
                Double durationSecondsVal = Double.parseDouble(durationSecondsStr);

                durationSeconds.setOutValue(durationSecondsVal);
                return Futures.immediateFuture(END);
            } catch (Exception e) {
                sendFailedToReadVideoChunkEvent(
                    attempt,
                    cameraName,
                    command,
                    videoChunkFileName,
                    heliumEventNotifier,
                    process.exitValue(),
                    stdoutOutput.toString(),
                    stderrOutput.toString()
                );
                return Futures.immediateFailedFuture(e);
            }
        } else {
            String stderr = stderrOutput.toString();
            sendFailedToReadVideoChunkEvent(
                    attempt,
                    cameraName,
                    command,
                    videoChunkFileName,
                    heliumEventNotifier,
                    process.exitValue(),
                    stdoutOutput.toString(),
                    stderr
            );
            return Futures.immediateFailedFuture(new Exception(stderr));
        }
    }

    // ----------------------------------------------------------------------------

    public static LocalDateTime getChunkDateTime(String videoChunkFileName) {
        String chunkDateTimeStr = videoChunkFileName.substring(videoChunkFileName.length() - 23, videoChunkFileName.length() - 4);
        return LocalDateTime.parse(chunkDateTimeStr, DATE_TIME_FORMATTER);
    }

    public static long getChunkUnixTime(String videoChunkFileName) {
        LocalDateTime chunkDateTime = getChunkDateTime(videoChunkFileName);
        return toUnixTime(chunkDateTime);
    }

    public static long toUnixTime(LocalDateTime chunkDateTime) {
        return chunkDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    static void sendFailedToReadVideoChunkEvent(
            int currentAttempt,
            String cameraName,
            String command,
            String videoChunkFileName,
            HeliumEventNotifier heliumEventNotifier,
            int exitValue,
            String stdoutOutput,
            String stderrOutput
    ) {
        String eventTitle = String.format("Failed to read video chunk duration File [%s]", videoChunkFileName);
        String eventDetails = String.format("Camera [%s] File [%s] Attempts [%d] Command [%s]%nExit code:%d%nstdout [%s]%nstderr [%s]", cameraName,
                videoChunkFileName, currentAttempt, command, exitValue, stdoutOutput, stderrOutput);
        try {
            long chunkUnixTime = getChunkUnixTime(videoChunkFileName);
            //We try to time this event accordingly to the time of the chunk we're trying to read, which can be read from chunk filename
            heliumEventNotifier.notifyEvent(chunkUnixTime, HeliumEventType.VIDEO_CHUNK_NOT_READABLE, cameraName,
                    eventTitle, eventDetails);
        } catch (Exception e) {
            //Or if that fails, current time
            heliumEventNotifier.notifyEvent(HeliumEventType.VIDEO_CHUNK_NOT_READABLE, cameraName,
                    eventTitle, eventDetails);
        }
    }
}
