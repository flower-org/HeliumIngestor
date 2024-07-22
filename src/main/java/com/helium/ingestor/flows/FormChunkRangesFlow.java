package com.helium.ingestor.flows;

import com.flower.anno.event.DisableEventProfiles;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.InOut;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.common.Output;
import com.flower.anno.params.step.FlowFactory;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.FlowFactoryPrm;
import com.flower.conf.FlowFuture;
import com.flower.conf.InOutPrm;
import com.flower.conf.NullableInOutPrm;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import com.flower.utilities.FuturesTool;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.helium.ingestor.core.HeliumEventNotifier;
import com.helium.ingestor.core.HeliumEventType;
import com.helium.ingestor.flows.events.FlowTerminationEvent;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.helium.ingestor.HeliumIngestorService.HELIUM_INGESTOR;
import static com.helium.ingestor.flows.AnalyzeAndMergeChunkRangeFlow.getRangeDuration;
import static com.helium.ingestor.flows.AnalyzeAndMergeChunkRangeFlow.notifyGapAndSurplusReport;
import static com.helium.ingestor.flows.AnalyzeAndMergeChunkRangeFlow.notifySurplusReport;
import static com.helium.ingestor.flows.AnalyzeAndMergeChunkRangeFlow.secondsAsDoubleToDuration;
import static com.helium.ingestor.flows.LoadChunkVideoDurationFlow.LOGGER;
import static com.helium.ingestor.flows.LoadChunkVideoDurationFlow.getChunkDateTime;
import static com.helium.ingestor.flows.LoadChunkVideoDurationFlow.getChunkUnixTime;

@FlowType(firstStep = "INIT_FLOW")
@DisableEventProfiles({FlowTerminationEvent.class})
public class FormChunkRangesFlow {
    @State final String cameraName;
    @State final boolean cameraHasAudio;
    @State final List<VideoChunkManagerFlow.ChunkInfo> chunksToMerge;
    @State final VideoChunkManagerFlow.ChunkInfo nextHourChunk;
    @State final Set<VideoChunkManagerFlow.ChunkInfo> badChunks;
    @State final List<AnalyzeAndMergeChunkRangeFlow.ChunkRangeInfo> chunksContiguousRanges;
    @State final HeliumEventNotifier heliumEventNotifier;
    @State final static int GAP_IN_FOOTAGE_SIZE_MS = 1500;

    @State @Nullable Iterator<VideoChunkManagerFlow.ChunkInfo> chunksToMergeIterator;
    @State @Nullable CurrentRangeInfo currentRange = null;
    @State @Nullable VideoChunkManagerFlow.ChunkInfo currentChunk = null;

    public static class CurrentRangeInfo {
        List<VideoChunkManagerFlow.ChunkInfo> range = new ArrayList<>();
        boolean isCurrentRangeNoAudio = false;
        @Nullable LocalDateTime currentRangeWm = null;
        @Nullable Duration actualRangeDuration = null;
    }

    public FormChunkRangesFlow(String cameraName,
                               boolean cameraHasAudio,
                               List<VideoChunkManagerFlow.ChunkInfo> chunksToMerge,
                               VideoChunkManagerFlow.ChunkInfo nextHourChunk,
                               Set<VideoChunkManagerFlow.ChunkInfo> badChunks,
                               List<AnalyzeAndMergeChunkRangeFlow.ChunkRangeInfo> chunksContiguousRanges,
                               HeliumEventNotifier heliumEventNotifier) {
        this.cameraName = cameraName;
        this.cameraHasAudio = cameraHasAudio;
        this.chunksToMerge = chunksToMerge;
        this.nextHourChunk = nextHourChunk;
        this.badChunks = badChunks;
        this.chunksContiguousRanges = chunksContiguousRanges;
        this.heliumEventNotifier = heliumEventNotifier;
    }

    @SimpleStepFunction
    public static Transition INIT_FLOW(
            @In List<VideoChunkManagerFlow.ChunkInfo> chunksToMerge,
            @Out OutPrm<Iterator<VideoChunkManagerFlow.ChunkInfo>> chunksToMergeIterator,
            @StepRef Transition FORM_RANGES
    ) {
        chunksToMergeIterator.setOutValue(chunksToMerge.iterator());
        return FORM_RANGES;
    }

    @SimpleStepFunction
    public static Transition FORM_RANGES(@In Set<VideoChunkManagerFlow.ChunkInfo> badChunks,
                                         @In Iterator<VideoChunkManagerFlow.ChunkInfo> chunksToMergeIterator,

                                         @In @Nullable CurrentRangeInfo currentRange,
                                         @Out(out = Output.OPTIONAL) OutPrm<VideoChunkManagerFlow.ChunkInfo> currentChunk,

                                         @StepRef Transition FINALIZE_RANGES,
                                         @StepRef Transition START_NEW_RANGE,
                                         @StepRef Transition ADD_TO_RANGE,

                                         @StepRef Transition FORM_RANGES) {
        if (!chunksToMergeIterator.hasNext()) {
            // No more chunks, finalize
            return FINALIZE_RANGES;
        } else {
            VideoChunkManagerFlow.ChunkInfo chunkInfo = chunksToMergeIterator.next();
            //Ignore bad chunks
            if (badChunks.contains(chunkInfo)) {
                return FORM_RANGES;
            }

            currentChunk.setOutValue(chunkInfo);
            if (currentRange == null) {
                // start new range with this chunk
                return START_NEW_RANGE;
            } else {
                //add chunk to the existing range
                return ADD_TO_RANGE;
            }
        }
    }

    @SimpleStepFunction
    public static ListenableFuture<Transition> START_NEW_RANGE(@In String cameraName,
                                                               @In boolean cameraHasAudio,
                                                               @In Set<VideoChunkManagerFlow.ChunkInfo> badChunks,
                                                               @In HeliumEventNotifier heliumEventNotifier,
                                                               @FlowFactory FlowFactoryPrm<LoadMediaChannelsFlow> loadMediaChannelsFlowFactory,

                                                               @InOut NullableInOutPrm<CurrentRangeInfo> currentRange,
                                                               @In(throwIfNull = true) VideoChunkManagerFlow.ChunkInfo currentChunk,

                                                               @StepRef Transition FORM_RANGES) {
        Duration chunkDuration = secondsAsDoubleToDuration(checkNotNull(currentChunk.chunkDuration));

        //First chunk of new range
        if (!cameraHasAudio) {
            //If camera has no audio, initialize the first range without audio
            CurrentRangeInfo newCurrentRangeVal = new CurrentRangeInfo();
            newCurrentRangeVal.range.add(currentChunk);
            newCurrentRangeVal.currentRangeWm = getChunkDateTime(currentChunk.chunkFile.getName()).plus(chunkDuration);
            newCurrentRangeVal.actualRangeDuration = chunkDuration;
            newCurrentRangeVal.isCurrentRangeNoAudio = true;

            currentRange.setOutValue(newCurrentRangeVal);
            return Futures.immediateFuture(FORM_RANGES);
        } else {
            //Otherwise, if camera has audio - check audio on chunk
            LoadMediaChannelsFlow loadMediaChannelsFlow = new LoadMediaChannelsFlow(cameraName,
                    currentChunk.chunkFile, heliumEventNotifier);
            FlowFuture<LoadMediaChannelsFlow> flowFuture = loadMediaChannelsFlowFactory.runChildFlow(loadMediaChannelsFlow);

            return FuturesTool.tryCatch(flowFuture.getFuture(),
                    loadMediaChannelsFlowResult -> {
                        //Initialize the first range with audio check result
                        CurrentRangeInfo newCurrentRangeVal = new CurrentRangeInfo();
                        newCurrentRangeVal.range.add(currentChunk);
                        newCurrentRangeVal.currentRangeWm = getChunkDateTime(currentChunk.chunkFile.getName()).plus(chunkDuration);
                        newCurrentRangeVal.actualRangeDuration = chunkDuration;
                        if (!loadMediaChannelsFlowResult.hasAudio) {
                            newCurrentRangeVal.isCurrentRangeNoAudio = true;
                        }

                        currentRange.setOutValue(newCurrentRangeVal);
                        return FORM_RANGES;
                    },
                    Exception.class,
                    e -> {
                        // Can't get channel info - bad chunk, add to bad chunks
                        badChunks.add(currentChunk);

                        //No need to finalize range since its empty

                        //report bad chunk?
                        String videoChunkFileName = currentChunk.chunkFile.getAbsolutePath();
                        String stderrOutput = getStackTraceAsString(e);
                        String eventTitle = String.format("Failed to read video chunk channels File [%s]", videoChunkFileName);
                        String eventDetails = String.format("Camera [%s] File [%s]%nerror [%s]",
                                cameraName, videoChunkFileName, stderrOutput);

                        long chunkUnixTime = getChunkUnixTime(videoChunkFileName);
                        heliumEventNotifier.notifyEvent(System.currentTimeMillis(), HELIUM_INGESTOR, chunkUnixTime,
                                HeliumEventType.VIDEO_CHUNK_NOT_READABLE, cameraName, eventTitle, eventDetails);

                        return FORM_RANGES;
                    },
                    MoreExecutors.directExecutor());
        }
    }

    @SimpleStepFunction
    public static ListenableFuture<Transition> ADD_TO_RANGE(@In String cameraName,
                                                           @In boolean cameraHasAudio,
                                                           @In Set<VideoChunkManagerFlow.ChunkInfo> badChunks,
                                                           @In List<AnalyzeAndMergeChunkRangeFlow.ChunkRangeInfo> chunksContiguousRanges,
                                                           @In HeliumEventNotifier heliumEventNotifier,
                                                           @FlowFactory FlowFactoryPrm<LoadMediaChannelsFlow> loadMediaChannelsFlowFactory,

                                                           @InOut(throwIfNull = true, out = Output.OPTIONAL) InOutPrm<CurrentRangeInfo> currentRange,
                                                           @In(throwIfNull = true) VideoChunkManagerFlow.ChunkInfo currentChunk,

                                                           @StepRef Transition START_NEW_RANGE,
                                                           @StepRef Transition FORM_RANGES) {
        {
            Duration chunkDuration = secondsAsDoubleToDuration(checkNotNull(currentChunk.chunkDuration));
            CurrentRangeInfo currentRangeVal = currentRange.getInValue();

            //Continuing existing range
            LocalDateTime chunkStart = getChunkDateTime(currentChunk.chunkFile.getName());
            Duration distance = Duration.between(currentRangeVal.currentRangeWm, chunkStart);

            //Check if time gap was found
            if (distance.toMillis() >= GAP_IN_FOOTAGE_SIZE_MS) {
                // Gap inside the merge range found. Reporting event: GAP_IN_FOOTAGE
                notifyGapAndSurplusReport(cameraName, currentRangeVal.range, currentChunk,
                        checkNotNull(currentRangeVal.currentRangeWm), distance, heliumEventNotifier,
                        checkNotNull(currentRangeVal.actualRangeDuration));

                // Finishing the range:
                // Add previous range to ranges
                chunksContiguousRanges.add(new AnalyzeAndMergeChunkRangeFlow.ChunkRangeInfo(currentRangeVal.range,
                        currentRangeVal.currentRangeWm));
                currentRange.setOutValue(null);

                //Starting a new range with existing chunk
                return Futures.immediateFuture(START_NEW_RANGE);
            } else {
                //If gap not found, try to add new chunk to the current range
                boolean cameraHasAudioAndCurrentRangeDoesnt = currentRangeVal.isCurrentRangeNoAudio && cameraHasAudio;
                if (!cameraHasAudioAndCurrentRangeDoesnt) {
                    // If audio is in sync with camera settings, we add the chunk to the range.
                    currentRangeVal.range.add(currentChunk);
                    currentRangeVal.currentRangeWm = getChunkDateTime(currentChunk.chunkFile.getName()).plus(chunkDuration);
                    currentRangeVal.actualRangeDuration = checkNotNull(currentRangeVal.actualRangeDuration).plus(chunkDuration);
                    return Futures.immediateFuture(FORM_RANGES);
                } else {
                    // We might need to finish the current range in a rare situation when
                    // it's a "NoAudio" range on a camera supporting audio,
                    // in case we discover that the next chunk has audio.
                    // Because if we merge a range in which the first chunk has no audio and the rest have audio,
                    // the whole merged video will have no audio (audio on good chunks will be lost).

                    //Check audio on chunk
                    LoadMediaChannelsFlow loadMediaChannelsFlow = new LoadMediaChannelsFlow(cameraName,
                            currentChunk.chunkFile, heliumEventNotifier);
                    FlowFuture<LoadMediaChannelsFlow> flowFuture = loadMediaChannelsFlowFactory.runChildFlow(loadMediaChannelsFlow);

                    return FuturesTool.tryCatch(flowFuture.getFuture(),
                            loadMediaChannelsFlowResult -> {
                                if (loadMediaChannelsFlowResult.hasAudio) {
                                    //If there is audio we start new range.
                                    // Finishing the range:
                                    // Add previous range to ranges
                                    chunksContiguousRanges.add(new AnalyzeAndMergeChunkRangeFlow.ChunkRangeInfo(currentRangeVal.range,
                                            checkNotNull(currentRangeVal.currentRangeWm)));
                                    currentRange.setOutValue(null);

                                    //Starting a new range with existing chunk
                                    return START_NEW_RANGE;
                                } else {
                                    //If there is still no audio, we continue the no-audio range.
                                    currentRangeVal.range.add(currentChunk);
                                    currentRangeVal.currentRangeWm = getChunkDateTime(currentChunk.chunkFile.getName()).plus(chunkDuration);
                                    currentRangeVal.actualRangeDuration = checkNotNull(currentRangeVal.actualRangeDuration).plus(chunkDuration);
                                    return FORM_RANGES;
                                }
                            },
                            Exception.class,
                            e -> {
                                // Can't get channel info, bad chunk, add to bad chunks;
                                badChunks.add(currentChunk);

                                // Since this is a gap, start a new range
                                chunksContiguousRanges.add(new AnalyzeAndMergeChunkRangeFlow.ChunkRangeInfo(currentRangeVal.range,
                                        checkNotNull(currentRangeVal.currentRangeWm)));
                                currentRange.setOutValue(null);

                                //report bad chunk?
                                String videoChunkFileName = currentChunk.chunkFile.getAbsolutePath();
                                String stderrOutput = getStackTraceAsString(e);
                                String eventTitle = String.format("Failed to read video chunk channels File [%s]", videoChunkFileName);
                                String eventDetails = String.format("Camera [%s] File [%s]%nerror [%s]",
                                        cameraName, videoChunkFileName, stderrOutput);

                                long chunkUnixTime = getChunkUnixTime(videoChunkFileName);
                                heliumEventNotifier.notifyEvent(System.currentTimeMillis(), HELIUM_INGESTOR, chunkUnixTime,
                                        HeliumEventType.VIDEO_CHUNK_NOT_READABLE, cameraName, eventTitle, eventDetails);
                                return FORM_RANGES;
                            },
                            MoreExecutors.directExecutor());
                }
            }
        }
    }

    @SimpleStepFunction
    public static ListenableFuture<Transition> FINALIZE_RANGES(@In String cameraName,
                                                               @In VideoChunkManagerFlow.ChunkInfo nextHourChunk,
                                                               @In List<AnalyzeAndMergeChunkRangeFlow.ChunkRangeInfo> chunksContiguousRanges,
                                                               @In HeliumEventNotifier heliumEventNotifier,
                                                               @In @Nullable CurrentRangeInfo currentRange,
                                                               @Terminal Transition END) {
        // No more chunks, finalize
        if (currentRange != null) {
            // Determine if there is a gap between the last range and next hour, and report if found
            LocalDateTime nextHourChunkStart = getChunkDateTime(nextHourChunk.chunkFile.getName());
            Duration distance = Duration.between(checkNotNull(currentRange.currentRangeWm), nextHourChunkStart);
            if (distance.toMillis() >= GAP_IN_FOOTAGE_SIZE_MS) {
                // Gap inside the merge range found. Reporting event: GAP_IN_FOOTAGE
                notifyGapAndSurplusReport(cameraName, currentRange.range, nextHourChunk,
                        currentRange.currentRangeWm, distance, heliumEventNotifier,
                        checkNotNull(currentRange.actualRangeDuration));
            } else {
                Duration rangeDuration = getRangeDuration(currentRange.range, nextHourChunk);
                notifySurplusReport(cameraName, currentRange.range, nextHourChunk, heliumEventNotifier,
                        rangeDuration, checkNotNull(currentRange.actualRangeDuration));
            }

            // Add last range to ranges
            chunksContiguousRanges.add(new AnalyzeAndMergeChunkRangeFlow.ChunkRangeInfo(currentRange.range,
                    currentRange.currentRangeWm));
        }

        return Futures.immediateFuture(END);
    }
}
