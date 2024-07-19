package com.helium.ingestor.flows;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.helium.ingestor.flows.AnalyzeAndMergeChunkRangeFlow.GAP_IN_FOOTAGE_SIZE_MS;
import static com.helium.ingestor.flows.AnalyzeAndMergeChunkRangeFlow.secondsAsDoubleToDuration;
import static com.helium.ingestor.flows.CameraProcessRunnerFlow.readString;
import static com.helium.ingestor.flows.LoadChunkVideoDurationFlow.getChunkDateTime;
import static com.helium.ingestor.flows.LoadChunkVideoDurationFlow.getChunkUnixTime;
import static com.helium.ingestor.flows.LoadChunkVideoDurationFlow.toUnixTime;
import static com.helium.ingestor.flows.VideoChunkManagerFlow.ChunkInfo;

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
import com.flower.anno.params.step.FlowFactory;
import com.flower.anno.params.transit.InRetOrException;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.FlowFactoryPrm;
import com.flower.conf.FlowFuture;
import com.flower.conf.InOutPrm;
import com.flower.conf.OutPrm;
import com.flower.conf.ReturnValueOrException;
import com.flower.conf.Transition;
import com.flower.utilities.FuturesTool;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.helium.ingestor.core.HeliumEventNotifier;
import com.helium.ingestor.core.HeliumEventType;
import com.helium.ingestor.flows.events.FlowTerminationEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.helium.ingestor.HeliumIngestorService.HELIUM_INGESTOR;
import static com.helium.ingestor.flows.AnalyzeAndMergeChunkRangeFlow.ChunkRangeInfo;

// TODO: DRY With LoadVideoDurationFlow, LoadVideoChannelsFlow
@FlowType(firstStep = "INIT_PROCESS")
@DisableEventProfiles({FlowTerminationEvent.class})
public class MergeChunkSubRangeFlow {
    final static Logger LOGGER = LoggerFactory.getLogger(MergeChunkSubRangeFlow.class);

    final static DateTimeFormatter MERGED_FILE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss");

    /** Stitches videos without re-encoding them, with the concat protocol (fast). */
//    final static String CMD = "ffmpeg -i \"concat:%s\" -c copy %s"; This didn't work
    final static String CMD = "ffmpeg -y -f concat -safe 0 -i %s -c copy %s";

    final static Integer MAX_RETRIES = 3;

    @State final ChunkRangeInfo chunkRangeInfo;
    @State final String cameraName;
    @State final boolean cameraHasAudio;
    @State final boolean cameraHasVideo;
    @State final boolean debugOutputMergeChunkList;
    @State final List<ChunkInfo> chunksToMerge;
    @State final LocalDateTime endOfRange;
    @State final HeliumEventNotifier heliumEventNotifier;

    @State final StringBuilder stdoutOutput;
    @State final StringBuilder stderrOutput;

    @State final File outputFolder;

    @State @Nullable File outputFile;
    /** This file is created as temp and is used as a parameter to ffmpeg concat */
    @State @Nullable File chunksFile;
    @State @Nullable String command;

    @State @Nullable Process process;
    @State @Nullable BufferedReader stdout;
    @State @Nullable BufferedReader stderr;
    @State int attempt = 1;

    static String formChunkFileContent(List<ChunkInfo> chunksToMerge) {
        StringBuilder chunksFileContent = new StringBuilder();
        chunksToMerge.forEach(
                chunk -> {
                    chunksFileContent.append(String.format("file '%s'%n", chunk.chunkFile.getAbsolutePath()));
                });
        return chunksFileContent.toString();
    }

    public MergeChunkSubRangeFlow(String cameraName,
                                  boolean cameraHasAudio,
                                  boolean cameraHasVideo,
                                  boolean debugOutputMergeChunkList,
                                  ChunkRangeInfo chunkRangeInfo,
                                  File outputFolder,
                                  HeliumEventNotifier heliumEventNotifier) {
        this.cameraName = cameraName;
        this.debugOutputMergeChunkList = debugOutputMergeChunkList;
        this.chunkRangeInfo = chunkRangeInfo;
        this.chunksToMerge = chunkRangeInfo.chunkRange;
        this.endOfRange = chunkRangeInfo.endOfRange;
        this.heliumEventNotifier = heliumEventNotifier;
        this.stdoutOutput = new StringBuilder();
        this.stderrOutput = new StringBuilder();
        this.outputFolder = outputFolder;
        this.cameraHasAudio = cameraHasAudio;
        this.cameraHasVideo = cameraHasVideo;
    }

    @SimpleStepFunction
    public static Transition INIT_PROCESS(
            @In String cameraName,
            @In File outputFolder,
            @In List<ChunkInfo> chunksToMerge,
            @In LocalDateTime endOfRange,
            @In HeliumEventNotifier heliumEventNotifier,

            @Out OutPrm<File> outputFile,
            @Out OutPrm<File> chunksFile,
            @Out OutPrm<String> command,

            @StepRef Transition LAUNCH_PROCESS
    ) throws IOException {
        LocalDateTime startOfRange = getChunkDateTime(chunksToMerge.get(0).chunkFile.getName());
        File outputFileVal = new File(outputFolder,
                String.format("%s_merged_%s_%s.mp4",
                        cameraName,
                        MERGED_FILE_DATE_TIME_FORMATTER.format(startOfRange),
                        MERGED_FILE_DATE_TIME_FORMATTER.format(endOfRange)));

        // Check for pre-existing merge output file
        if (outputFileVal.exists()) {
            // Found pre-existing merge output file, throw event
            // Get filename to rename pre-existing file to (e.g. "file.mp4" -> "file_1.mp4")
            File renameTo = getRenameFileName(outputFileVal);

            // Pre-existing output file about to be renamed, throw event
            String eventTitleRename = String.format("Renaming pre-existing merge output file: [%s], renaming to [%s]",
                    outputFileVal.getAbsolutePath(), renameTo.getAbsolutePath());
            heliumEventNotifier.notifyEvent(HELIUM_INGESTOR, HeliumEventType.FOUND_PRE_EXISTING_MERGE_OUTPUT_FILE, cameraName,
                    eventTitleRename, eventTitleRename);

            outputFileVal.renameTo(renameTo);
        }

        // Form merge command
        String chunksFileContent = formChunkFileContent(chunksToMerge);

        FileAttribute<Set<PosixFilePermission>> allCanRead = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        File chunksFileVal = Files.createTempFile("HeliumIngestor", "-merge-job", allCanRead).toFile();
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(chunksFileVal), StandardCharsets.UTF_8)) {
            writer.write(chunksFileContent);
        }
        chunksFile.setOutValue(chunksFileVal);

        String commandVal = String.format(CMD, chunksFileVal.getAbsolutePath(), outputFileVal.getAbsolutePath());

        outputFile.setOutValue(outputFileVal);
        command.setOutValue(commandVal);

        return LAUNCH_PROCESS;
    }

    @StepFunction(transit = "LAUNCH_PROCESS_TRANSITION")
    public static void LAUNCH_PROCESS(
            @In String command,
            @In boolean debugOutputMergeChunkList,
            @In List<ChunkInfo> chunksToMerge,
            @Out OutPrm<Process> process,
            @Out OutPrm<BufferedReader> stdout,
            @Out OutPrm<BufferedReader> stderr
    ) throws IOException {
        String firstChunk = "chunk N/A", lastChunk = "chunk N/A";
        if (!chunksToMerge.isEmpty()) {
            firstChunk = chunksToMerge.get(0).chunkFile.getName();
            lastChunk = chunksToMerge.get(chunksToMerge.size()-1).chunkFile.getName();
        }

        if (debugOutputMergeChunkList) {
            String chunksFileContent = formChunkFileContent(chunksToMerge);
            LOGGER.info("Merging {} video chunks ({} -> {}). cmd: {}\nChunksFile:-----\n{}\n----------------", chunksToMerge.size(),
                    firstChunk, lastChunk, command, chunksFileContent);
        } else {
            LOGGER.info("Merging {} video chunks ({} -> {}). cmd: {}", chunksToMerge.size(), firstChunk, lastChunk, command);
        }

        Process newProcess = Runtime.getRuntime().exec(command);

        stdout.setOutValue(new BufferedReader(new InputStreamReader(newProcess.getInputStream())));
        stderr.setOutValue(new BufferedReader(new InputStreamReader(newProcess.getErrorStream())));
        process.setOutValue(newProcess);
    }

    @TransitFunction
    public static Transition LAUNCH_PROCESS_TRANSITION(
            @In String cameraName,
            @In String command,
            @In LocalDateTime endOfRange,
            @In HeliumEventNotifier heliumEventNotifier,
            @InOut(throwIfNull = true, out = Output.OPTIONAL) InOutPrm<Integer> attempt,
            @InRetOrException ReturnValueOrException<Void> retValOrExc,
            @StepRef(desc = "If merge process started successfully,\n" +
                    "we proceed to read its STDOUT/ERR")
                Transition READ_PROCESS_OUTPUT,
            @StepRef(desc = "If exception was encountered while starting\n" +
                    "merge process, we retry up to 3 times")
                Transition LAUNCH_PROCESS,
            @Terminal(desc = "If max retries was exceeded,\n" +
                    "we terminate the flow")
                Transition END
    ) {
        if (retValOrExc.exception().isPresent()) {
            int currentAttempt = attempt.getInValue();
            LOGGER.error("Error running video chunk merge: attempt [{}] camera [{}]", currentAttempt,
                    cameraName, retValOrExc.exception().get());

            if (currentAttempt < MAX_RETRIES) {
                int delay = 500;//ms
                attempt.setOutValue(currentAttempt + 1);
                return LAUNCH_PROCESS.setDelay(Duration.ofMillis(delay));
            } else {
                sendFailedToMergeVideoChunksEvent(currentAttempt, cameraName, command, endOfRange, heliumEventNotifier, -1, "N/A", "N/A");
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

            @StepRef(desc = "While the process is alive,\n" +
                    "we keep reading its STDOUT/ERR")
                Transition READ_PROCESS_OUTPUT,
            @StepRef(desc = "If process terminated,\n" +
                    "we proceed to parse the collected output")
                Transition PARSE_OUTPUT
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
            @In File chunksFile,

            @In int attempt,
            @In String cameraName,
            @In String command,
            @In LocalDateTime endOfRange,
            @In HeliumEventNotifier heliumEventNotifier,

            @StepRef(desc = "If the merge process finished successfully,\n" +
                    "run pos-merge checks")
                Transition POST_MERGE_DURATION_CHECK
    ) {
        if (process.exitValue() == 0) {
            chunksFile.delete();
            return Futures.immediateFuture(POST_MERGE_DURATION_CHECK);
        } else {
            String stderr = stderrOutput.toString();
            sendFailedToMergeVideoChunksEvent(
                    attempt,
                    cameraName,
                    command,
                    endOfRange,
                    heliumEventNotifier,
                    process.exitValue(),
                    stdoutOutput.toString(),
                    stderr
            );
            return Futures.immediateFailedFuture(new Exception(stderr));
        }
    }

    @SimpleStepFunction
    public static ListenableFuture<Transition> POST_MERGE_DURATION_CHECK(
            @In File outputFile,
            @In ChunkRangeInfo chunkRangeInfo,

            @In String cameraName,
            @In LocalDateTime endOfRange,
            @In HeliumEventNotifier heliumEventNotifier,

            @FlowFactory(desc = "Run `GetDuration` operation\n"+
                    "for a video in a child Flow",
                    flowType = LoadVideoDurationFlow.class)
                FlowFactoryPrm<LoadVideoDurationFlow> loadVideoDurationFlowFactory,

            @StepRef(desc = "After duration check, check that both\n" +
                            "audio and video channels are present")
                Transition POST_MERGE_AUDIO_VIDEO_INTEGRITY_CHECK
    ) {
        LoadVideoDurationFlow loadVideoDurationFlow =
                new LoadVideoDurationFlow(cameraName, outputFile.getAbsolutePath(), heliumEventNotifier);
        FlowFuture<LoadVideoDurationFlow> flowFuture = loadVideoDurationFlowFactory.runChildFlow(loadVideoDurationFlow);

        return FuturesTool.tryCatch(flowFuture.getFuture(),
                flow -> {
                    if (flow.durationException == null) {
                        LocalDateTime startTime = getChunkDateTime(chunkRangeInfo.chunkRange.get(0).chunkFile.getName());
                        LocalDateTime endTime = chunkRangeInfo.endOfRange;
                        Duration distance = Duration.between(startTime, endTime);
                        long rangeDurationMillis = distance.toMillis();
                        long videoDurationMillis = secondsAsDoubleToDuration(checkNotNull(flow.durationSeconds)).toMillis();
                        if (Math.abs(rangeDurationMillis - videoDurationMillis) >= GAP_IN_FOOTAGE_SIZE_MS) {
                            String error = String.format("Range: %s. Duration discrepancy: Range duration ms: %d; Video duration ms: %d",
                                    outputFile.getName(), rangeDurationMillis, videoDurationMillis);
                            if (rangeDurationMillis - videoDurationMillis >= GAP_IN_FOOTAGE_SIZE_MS) {
                                sendDurationCheckVideoChunksEvent(cameraName, endOfRange, heliumEventNotifier, error);
                                chunkRangeInfo.retainChunks = true;
                            } else {
                                LOGGER.warn("Footage surplus: {} {}", cameraName, error);
                            }
                        }
                    } else {
                        String error = getStackTraceAsString(flow.durationException);
                        sendDurationCheckVideoChunksEvent(cameraName, endOfRange, heliumEventNotifier, error);
                        chunkRangeInfo.retainChunks = true;
                    }
                    return POST_MERGE_AUDIO_VIDEO_INTEGRITY_CHECK;
                },
                Exception.class,
                e -> {
                    String error = getStackTraceAsString(e);
                    sendDurationCheckVideoChunksEvent(cameraName, endOfRange, heliumEventNotifier, error);
                    chunkRangeInfo.retainChunks = true;
                    return POST_MERGE_AUDIO_VIDEO_INTEGRITY_CHECK;
                },
                MoreExecutors.directExecutor());

    }

    @SimpleStepFunction
    public static ListenableFuture<Transition> POST_MERGE_AUDIO_VIDEO_INTEGRITY_CHECK(
            @In File outputFile,
            @In ChunkRangeInfo chunkRangeInfo,
            @In String cameraName,
            @In boolean cameraHasAudio,
            @In boolean cameraHasVideo,
            @In LocalDateTime endOfRange,
            @In HeliumEventNotifier heliumEventNotifier,

            @FlowFactory(desc = "Run `GetChannels` operation\n" +
                    "for a video in a child Flow",
                    flowType = LoadMediaChannelsFlow.class)
                FlowFactoryPrm<LoadMediaChannelsFlow> loadMediaChannelsFlowFactory,

            @Terminal Transition END
    ) {
        LoadMediaChannelsFlow loadMediaChannelsFlow =
                new LoadMediaChannelsFlow(cameraName, outputFile.getAbsolutePath(), heliumEventNotifier);
        FlowFuture<LoadMediaChannelsFlow> flowFuture = loadMediaChannelsFlowFactory.runChildFlow(loadMediaChannelsFlow);

        return FuturesTool.tryCatch(flowFuture.getFuture(),
            flow -> {
                if (flow.channelsException == null) {
                    if ((!flow.hasAudio && cameraHasAudio) || (!flow.hasVideo && cameraHasVideo)) {
                        chunkRangeInfo.retainChunks = true;
                        String error = String.format("Range: %s. flow.hasAudio %s; cameraHasAudio %s; flow.hasVideo %s; cameraHasVideo %s",
                                outputFile.getName(), flow.hasAudio, cameraHasAudio, flow.hasVideo, cameraHasVideo);
                        sendAudioVideoChannelCheckVideoChunksEvent(cameraName, endOfRange, heliumEventNotifier, error);
                    }
                } else {
                    String error = getStackTraceAsString(flow.channelsException);
                    sendAudioVideoChannelCheckVideoChunksEvent(cameraName, endOfRange, heliumEventNotifier, error);
                    chunkRangeInfo.retainChunks = true;
                }
                return END;
            },
            Exception.class,
            e -> {
                String error = getStackTraceAsString(e);
                sendAudioVideoChannelCheckVideoChunksEvent(cameraName, endOfRange, heliumEventNotifier, error);
                chunkRangeInfo.retainChunks = true;
                return END;
            },
            MoreExecutors.directExecutor());
    }

    // --------------------------------------------------------------------------------------

    public static File getRenameFileName(File oldFile) {
        int i = 0;
        while (true) {
            i++;
            String fullName = oldFile.getName();

            int dotIndex = fullName.lastIndexOf(".");
            String name = (dotIndex == -1) ? fullName : fullName.substring(0, dotIndex);
            String ext = (dotIndex == -1) ? "" : fullName.substring(dotIndex);

            String newName = String.format("%s_%d%s", name, i, ext);
            File newFileName = new File(oldFile.getParentFile(), newName);

            if (!newFileName.exists()) {
                return newFileName;
            }
        }
    }

    static void sendFailedToMergeVideoChunksEvent(
            int currentAttempt,
            String cameraName,
            String command,
            LocalDateTime endOfRange,
            HeliumEventNotifier heliumEventNotifier,
            int exitValue,
            String stdoutOutput,
            String stderrOutput
    ) {
        String eventTitle = "Failed to merge video chunks";
        String eventDetails = String.format("Camera [%s] Attempts [%d]%nExit code:%d%nstdout [%s]%nstderr [%s]%nCommand [%s]", cameraName,
                currentAttempt, exitValue, stdoutOutput, stderrOutput, command);

        try {
            long endOfRangeUnixTime = toUnixTime(endOfRange);
            //We try to time this event accordingly to the end of merge range time
            heliumEventNotifier.notifyEvent(System.currentTimeMillis(), HELIUM_INGESTOR, endOfRangeUnixTime,
                    HeliumEventType.VIDEO_MERGING_FAILED, cameraName, eventTitle, eventDetails);
        } catch (Exception e) {
            //Or if that fails, current time
            heliumEventNotifier.notifyEvent(HELIUM_INGESTOR, HeliumEventType.VIDEO_MERGING_FAILED, cameraName,
                    eventTitle, eventDetails);
        }
    }

    static void sendAudioVideoChannelCheckVideoChunksEvent(
            String cameraName,
            LocalDateTime endOfRange,
            HeliumEventNotifier heliumEventNotifier,
            String error
    ) {
        String eventTitle = "Failed to check audio/video channel integrity. Retaining chunks.";
        String eventDetails = String.format("Error: [%s]", error);

        try {
            long endOfRangeUnixTime = toUnixTime(endOfRange);
            //We try to time this event accordingly to the end of merge range time
            heliumEventNotifier.notifyEvent(System.currentTimeMillis(), HELIUM_INGESTOR, endOfRangeUnixTime,
                    HeliumEventType.MERGED_FOOTAGE_MISSING_MEDIA_CHANNELS, cameraName, eventTitle, eventDetails);
        } catch (Exception e) {
            //Or if that fails, current time
            heliumEventNotifier.notifyEvent(HELIUM_INGESTOR, HeliumEventType.MERGED_FOOTAGE_MISSING_MEDIA_CHANNELS,
                    cameraName, eventTitle, eventDetails);
        }
    }

    static void sendDurationCheckVideoChunksEvent(
            String cameraName,
            LocalDateTime endOfRange,
            HeliumEventNotifier heliumEventNotifier,
            String error
    ) {
        String eventTitle = "Video duration check failed. Retaining chunks.";
        String eventDetails = String.format("Error: [%s]", error);

        try {
            long endOfRangeUnixTime = toUnixTime(endOfRange);
            //We try to time this event accordingly to the end of merge range time
            heliumEventNotifier.notifyEvent(System.currentTimeMillis(), HELIUM_INGESTOR, endOfRangeUnixTime,
                    HeliumEventType.MERGED_FOOTAGE_DURATION_MISMATCH, cameraName, eventTitle, eventDetails);
        } catch (Exception e) {
            //Or if that fails, current time
            heliumEventNotifier.notifyEvent(HELIUM_INGESTOR, HeliumEventType.MERGED_FOOTAGE_DURATION_MISMATCH,
                    cameraName, eventTitle, eventDetails);
        }
    }
}
