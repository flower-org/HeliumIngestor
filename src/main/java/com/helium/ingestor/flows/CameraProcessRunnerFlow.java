package com.helium.ingestor.flows;

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
import com.google.common.base.Strings;
import com.helium.ingestor.core.HeliumEventNotifier;
import com.helium.ingestor.core.HeliumEventType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.helium.ingestor.HeliumIngestorService.HELIUM_INGESTOR;

@FlowType(firstStep = "LAUNCH_PROCESS")
public class CameraProcessRunnerFlow {
    final static Logger LOGGER = LoggerFactory.getLogger(CameraProcessRunnerFlow.class);

    static final Integer MAX_POWER_OF_2 = 10;
    static final Integer RESET_RETRY_COUNT_AFTER_MINUTES = 3;
    static final Integer POST_LAUNCH_OUTPUT_WAIT_PERIOD_SECONDS = 5;
    static final Integer MISSING_OUTPUT_TIMEOUT_SECONDS = 1;

    public static class RetryInfo {
        int failedLaunchRetries;
        long lastLaunchTime;
        long lastOutputReadTime;

        RetryInfo(int failedLaunchRetries) {
            this.failedLaunchRetries = failedLaunchRetries;
            this.lastLaunchTime = System.currentTimeMillis();
            this.lastOutputReadTime = System.currentTimeMillis();
        }
    }

    @State final String cameraName;
    @State final String command;
    @State final HeliumEventNotifier heliumEventNotifier;
    @State final StringBuilder lastLogLines;
    @State final int maxLogBufferSize;
    @State final RetryInfo retryInfo;

    @State @Nullable Process process;
    @State @Nullable BufferedReader stdout;
    @State @Nullable BufferedReader stderr;

    public CameraProcessRunnerFlow(String cameraName, String command, HeliumEventNotifier heliumEventNotifier) {
        this(cameraName, command, heliumEventNotifier, 36);
    }

    public CameraProcessRunnerFlow(String cameraName, String command, HeliumEventNotifier heliumEventNotifier, int maxLogBufferSize) {
        this.cameraName = cameraName;
        this.command = command;
        this.heliumEventNotifier = heliumEventNotifier;
        this.retryInfo = new RetryInfo(0);
        this.lastLogLines = new StringBuilder();
        this.maxLogBufferSize = maxLogBufferSize;
    }

    @StepFunction(transit = "LAUNCH_PROCESS_TRANSITION")
    public static void LAUNCH_PROCESS(
            @In String command,
            @Out OutPrm<Process> process,
            @Out OutPrm<BufferedReader> stdout,
            @Out OutPrm<BufferedReader> stderr
    ) throws IOException {
        LOGGER.info("Starting process, cmd: {}", command);

        Process newProcess = Runtime.getRuntime().exec(command);

        stdout.setOutValue(new BufferedReader(new InputStreamReader(newProcess.getInputStream())));
        stderr.setOutValue(new BufferedReader(new InputStreamReader(newProcess.getErrorStream())));
        process.setOutValue(newProcess);
    }

    @TransitFunction
    public static Transition LAUNCH_PROCESS_TRANSITION(
            @In @Nullable Process process,
            @In RetryInfo retryInfo,
            @In String cameraName,
            @In HeliumEventNotifier heliumEventNotifier,
            @StepRef Transition LAUNCH_PROCESS,
            @StepRef Transition READ_PROCESS_OUTPUT,
            @InRetOrException ReturnValueOrException<Void> retValOrExc
    ) {
        retryInfo.lastLaunchTime = System.currentTimeMillis();
        int delay = (int)(10.0 * Math.pow(2.0, retryInfo.failedLaunchRetries > MAX_POWER_OF_2 ? MAX_POWER_OF_2 : retryInfo.failedLaunchRetries));

        if (retValOrExc.exception().isPresent()) {
            LOGGER.error("Error starting process", retValOrExc.exception().get());
            return LAUNCH_PROCESS.setDelay(Duration.ofMillis(delay));
        } else {
            String eventTitle = String.format("Camera process started (ffmpeg). pid [%s]", checkNotNull(process).pid());
            heliumEventNotifier.notifyEvent(HELIUM_INGESTOR, HeliumEventType.CAMERA_PROCESS_STARTED, cameraName,
                    eventTitle, null);
            return READ_PROCESS_OUTPUT;
        }
    }

    @SimpleStepFunction
    public static Transition READ_PROCESS_OUTPUT(
            @In(throwIfNull = true) Process process,
            @In String cameraName,
            @In RetryInfo retryInfo,
            @In StringBuilder lastLogLines,
            @In int maxLogBufferSize,
            @In(throwIfNull = true) BufferedReader stdout,
            @In(throwIfNull = true) BufferedReader stderr,
            @In HeliumEventNotifier heliumEventNotifier,
            @StepRef Transition CHECK_PROCESS_STATE
    ) throws IOException {
        int charsRead = readFromStream(stdout, "STDOUT: ", lastLogLines, maxLogBufferSize);
        charsRead += readFromStream(stderr, "STDERR: ", lastLogLines, maxLogBufferSize);

        if (charsRead > 0) {
            retryInfo.lastOutputReadTime = System.currentTimeMillis();
        } else {
            long now = System.currentTimeMillis();
            //If we're messing output from the process,
            //and If process launch grace period passed
            if (retryInfo.lastLaunchTime + Duration.ofSeconds(POST_LAUNCH_OUTPUT_WAIT_PERIOD_SECONDS).toMillis() < now) {
                //and If absence of output timeout elapsed as well
                if (retryInfo.lastOutputReadTime + Duration.ofSeconds(MISSING_OUTPUT_TIMEOUT_SECONDS).toMillis() < now) {
                    //Then Forcibly kill process
                    String eventTitle = String.format("Killing process forcibly due to absence of output. pid [%s]", process.pid());
                    LOGGER.warn(eventTitle);
                    heliumEventNotifier.notifyEvent(HELIUM_INGESTOR, HeliumEventType.CAMERA_PROCESS_FORCIBLY_KILLED, cameraName,
                            eventTitle, lastLogLines.toString());

                    process.destroyForcibly();

                    return CHECK_PROCESS_STATE.setDelay(Duration.ofMillis(100L));
                }
            }
            //NB: An alternative way of detecting output timeout would be to rely on detection creation of new files,
            // but it's more cumbersome and hard to implement.
        }

        return CHECK_PROCESS_STATE;
    }

    @SimpleStepFunction
    public static Transition CHECK_PROCESS_STATE(
            @In(throwIfNull = true) Process process,
            @In String cameraName,
            @In RetryInfo retryInfo,
            @In HeliumEventNotifier heliumEventNotifier,
            @In StringBuilder lastLogLines,
            @StepRef Transition READ_PROCESS_OUTPUT,
            @StepRef Transition LAUNCH_PROCESS
    ) {
        if (!process.isAlive()) {
            if (retryInfo.lastLaunchTime + Duration.ofMinutes(RESET_RETRY_COUNT_AFTER_MINUTES).toMillis() < System.currentTimeMillis()) {
                //If we held for more than 3 minutes, we reset the retry counter
                retryInfo.failedLaunchRetries = 0;
            }

            //TODO: get rid of `MAX_POWER_OF_2` in favor of MAX_RELAUNCH_DELAY
            //Currently it's ~4 sec max delay
            int delay = (int)(4.0 * Math.pow(2.0, retryInfo.failedLaunchRetries > MAX_POWER_OF_2 ? MAX_POWER_OF_2 : retryInfo.failedLaunchRetries));
            Duration delayDuration = Duration.ofMillis(delay);

            retryInfo.failedLaunchRetries++;
            String eventTitle = String.format("Camera process terminated [%d]", process.pid());
            String processTerminationSummary = String.format(
                    "Process terminated with exit value: %d; restarting. Process alias: %s Attempt #%d; Delay time: %s",
                    process.exitValue(), cameraName, retryInfo.failedLaunchRetries, delayDuration);
            heliumEventNotifier.notifyEvent(HELIUM_INGESTOR, HeliumEventType.CAMERA_PROCESS_TERMINATED,
                    eventTitle, processTerminationSummary, lastLogLines.toString());

            return LAUNCH_PROCESS.setDelay(delayDuration);
        } else {
            return READ_PROCESS_OUTPUT.setDelay(Duration.ofMillis(100L));
        }
    }

    // -----------------------------------------------------------

    public static String readString(BufferedReader reader) throws IOException {
        StringBuilder newLogs = new StringBuilder();
        int value;
        while (reader.ready() && (value = reader.read()) != -1) {
            newLogs.append((char)value);
        }
        return newLogs.toString();
    }

    static int readFromStream(BufferedReader reader, @Nullable String prefix, StringBuilder lastLogLines,
                              @Nullable Integer maxLogBufferSize) throws IOException {
        if (reader.ready()) {
            String newLogs = readString(reader);
            LOGGER.debug("{}{}", Strings.nullToEmpty(prefix), newLogs);

            lastLogLines.append(newLogs);
            if (maxLogBufferSize != null) {
                if (lastLogLines.length() > maxLogBufferSize) {
                    lastLogLines.delete(0, lastLogLines.length() - maxLogBufferSize);
                }
            }
            return newLogs.length();
        }
        return 0;
    }
}
