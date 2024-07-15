package com.helium.ingestor.flows;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.helium.ingestor.HeliumIngestorService.HELIUM_INGESTOR;
import static com.helium.ingestor.flows.LoadChunkDurationFlow.*;
import static com.helium.ingestor.flows.MergeChunkSubRangeFlow.getRenameFileName;
import static com.helium.ingestor.flows.VideoChunkManagerFlow.ChunkInfo;
import static com.helium.ingestor.flows.VideoChunkManagerFlow.ChunkState;

import com.flower.anno.event.DisableEventProfiles;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.InOut;
import com.flower.anno.params.step.FlowFactory;
import com.flower.anno.params.transit.StepRef;
import com.flower.anno.params.transit.Terminal;
import com.flower.conf.FlowFactoryPrm;
import com.flower.conf.FlowFuture;
import com.flower.conf.InOutPrm;
import com.flower.conf.Transition;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.helium.ingestor.core.HeliumEventNotifier;
import com.helium.ingestor.core.HeliumEventType;
import com.helium.ingestor.flows.events.FlowTerminationEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

@FlowType(firstStep = "INTEGRITY_CHECK")
@DisableEventProfiles({FlowTerminationEvent.class})
public class AnalyzeAndMergeChunkRangeFlow {
    final static Logger LOGGER = LoggerFactory.getLogger(AnalyzeAndMergeChunkRangeFlow.class);
    final static String CMD = "ffmpeg -i \"concat:%s\" -c copy %s";
    final static Integer MAX_RETRIES = 3;
    final static DateTimeFormatter ZIP_FILE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss");

    /** Even though the precision is to a second, experimentally it was determined that sometimes gaps slightly larger
     * than 1 second are being detected, but don't indicate an actual footage loss, resulting in false positives.
     * For that reason we start calling it a gap only if the distance is larger than 1.5 sec */
    final static int GAP_IN_FOOTAGE_SIZE_MS = 1500;

    public static class ChunkRangeInfo {
        final List<ChunkInfo> chunkRange;
        final LocalDateTime endOfRange;

        ChunkRangeInfo(List<ChunkInfo> chunkRange, LocalDateTime endOfRange) {
            this.chunkRange = chunkRange;
            this.endOfRange = endOfRange;
        }
    }

    @State final String cameraName;
    @State final File outputFolder;
    @State final HeliumEventNotifier heliumEventNotifier;

    @State final List<ChunkInfo> chunksToMerge;
    @State final ChunkInfo nextHourChunk;
    /** List of contiguous chunks and end of footage time */
    @State final List<ChunkRangeInfo> chunksContiguousRanges;
    @State final Set<ChunkInfo> badChunks;

    @State @Nullable int currentMergeIndex;

    public AnalyzeAndMergeChunkRangeFlow(String cameraName,
                                         File outputFolder,
                                         HeliumEventNotifier heliumEventNotifier,
                                         List<ChunkInfo> chunksToMerge,
                                         ChunkInfo nextHourChunk) {
        this.cameraName = cameraName;
        this.outputFolder = outputFolder;
        this.heliumEventNotifier = heliumEventNotifier;

        this.chunksToMerge = chunksToMerge;
        this.nextHourChunk = nextHourChunk;
        this.chunksContiguousRanges = new ArrayList<>();
        this.badChunks = new HashSet<>();
        this.currentMergeIndex = 0;
    }

    //TODO: this is potentially a very heavy step, it might block for tens of seconds with SHA256 checksum
    // While Adler32 is a bit faster, but it still takes a second or two for thousands of files on SSD.
    // This step needs to be rewritten into iterative step to avoid blocking worker threads for too long and let other
    // flows make progress at the same time.
    @SimpleStepFunction
    public static Transition INTEGRITY_CHECK(@In String cameraName,
                                             @In List<ChunkInfo> chunksToMerge,
                                             @In Set<ChunkInfo> badChunks,
                                             @In HeliumEventNotifier heliumEventNotifier,
                                             @StepRef Transition CONTINUITY_CHECK) {
        for (ChunkInfo chunkInfo : chunksToMerge) {
            if (chunkInfo.chunkState == ChunkState.DURATION_LOADED) {
                File chunkFile = chunkInfo.chunkFile;
                boolean fileSizeMismatch = chunkFile.length() != checkNotNull(chunkInfo.fileLength);
                //TODO: Add config switch for this
                //byte[] checksum = getChecksumForFile(chunkFile);
                //boolean shaMismatch = !Arrays.equals(checksum, chunkInfo.checksum);

                //We try to time the events accordingly to the time of the chunk we're trying to read, which can be read from chunk filename
                long chunkUnixTime = getChunkUnixTime(chunkFile.getName());
                if (fileSizeMismatch) {
                    String eventTitle = String.format("Video chunk file size mismatch. File [%s]", chunkFile.getName());
                    String eventDetails = String.format("Video chunk file size mismatch. File [%s] Old length [%s] New length [%s]",
                            chunkFile.getName(), chunkInfo.fileLength, chunkFile.length());
                    heliumEventNotifier.notifyEvent(System.currentTimeMillis(), HELIUM_INGESTOR, chunkUnixTime,
                            HeliumEventType.VIDEO_CHUNK_FILE_SIZE_MISMATCH, cameraName, eventTitle, eventDetails);
                }
                /*if (shaMismatch) {
                    String eventTitle = String.format("Video chunk file checksum mismatch. File [%s]", chunkFile.getName());
                    String eventDetails = String.format("Video chunk file checksum mismatch. File [%s] Old checksum [%s] New checksum [%s]",
                            chunkFile.getName(), byteArrayToHex(checkNotNull(chunkInfo.checksum)), byteArrayToHex(checksum));
                    heliumEventNotifier.notifyEvent(chunkUnixTime, HeliumEventType.VIDEO_CHUNK_CHECKSUM_MISMATCH, cameraName,
                            eventTitle, eventDetails);
                }*/
                if (fileSizeMismatch /*|| shaMismatch*/) {
                    badChunks.add(chunkInfo);
                }
            } else {
                badChunks.add(chunkInfo);
            }
        }
        return CONTINUITY_CHECK;
    }

    @SimpleStepFunction
    public static Transition CONTINUITY_CHECK(@In String cameraName,
                                              @In List<ChunkInfo> chunksToMerge,
                                              @In ChunkInfo nextHourChunk,
                                              @In Set<ChunkInfo> badChunks,
                                              @In List<ChunkRangeInfo> chunksContiguousRanges,
                                              @In HeliumEventNotifier heliumEventNotifier,
                                              @StepRef Transition LAUNCH_MERGE_PROCESSES,
                                              @StepRef Transition ZIP_BAD_CHUNKS) {
    List<ChunkInfo> currentRange = null;
    LocalDateTime currentRangeWm = null;
    Duration actualRangeDuration = null;

    for (ChunkInfo chunkInfo : chunksToMerge) {
      if (!badChunks.contains(chunkInfo)) {
          Duration chunkDuration = secondsAsDoubleToDuration(checkNotNull(chunkInfo.chunkDuration));
          if (currentRange == null) {
              //Initialize the first range
              currentRange = new ArrayList<>();
              currentRange.add(chunkInfo);
              currentRangeWm = getChunkDateTime(chunkInfo.chunkFile.getName()).plus(chunkDuration);
              actualRangeDuration = chunkDuration;
          } else {
              LocalDateTime chunkStart = getChunkDateTime(chunkInfo.chunkFile.getName());
              Duration distance = Duration.between(currentRangeWm, chunkStart);
              if (distance.toMillis() >= GAP_IN_FOOTAGE_SIZE_MS) {
                  // Gap inside the merge range found. Reporting event: GAP_IN_FOOTAGE
                  notifyGapAndSurplusReport(cameraName, currentRange, chunkInfo, checkNotNull(currentRangeWm),
                          distance, heliumEventNotifier, checkNotNull(actualRangeDuration));

                  // Add previous range to ranges
                  chunksContiguousRanges.add(new ChunkRangeInfo(currentRange, currentRangeWm));

                  // Start New range
                  currentRange = new ArrayList<>();
                  currentRange.add(chunkInfo);
                  currentRangeWm = getChunkDateTime(chunkInfo.chunkFile.getName()).plus(chunkDuration);
                  actualRangeDuration = chunkDuration;
              } else {
                  currentRange.add(chunkInfo);
                  currentRangeWm = getChunkDateTime(chunkInfo.chunkFile.getName()).plus(chunkDuration);
                  actualRangeDuration = checkNotNull(actualRangeDuration).plus(chunkDuration);
              }
          }
      }
    }

    if (currentRange != null) {
        // Determine if there is a gap between the last range and next hour, and report if found
        LocalDateTime nextHourChunkStart = getChunkDateTime(nextHourChunk.chunkFile.getName());
        Duration distance = Duration.between(checkNotNull(currentRangeWm), nextHourChunkStart);
        if (distance.toMillis() >= GAP_IN_FOOTAGE_SIZE_MS) {
            // Gap inside the merge range found. Reporting event: GAP_IN_FOOTAGE
            notifyGapAndSurplusReport(cameraName, currentRange, nextHourChunk, currentRangeWm, distance, heliumEventNotifier, checkNotNull(actualRangeDuration));
        } else {
            Duration rangeDuration = getRangeDuration(currentRange, nextHourChunk);
            notifySurplusReport(cameraName, currentRange, nextHourChunk, heliumEventNotifier, rangeDuration, checkNotNull(actualRangeDuration));
        }

        // Add last range to ranges
        chunksContiguousRanges.add(new ChunkRangeInfo(currentRange, currentRangeWm));

        return LAUNCH_MERGE_PROCESSES;
    } else {
        return ZIP_BAD_CHUNKS;
    }
  }

  @SimpleStepFunction
  public static ListenableFuture<Transition> LAUNCH_MERGE_PROCESSES(@In String cameraName,
                                                                   @In List<ChunkRangeInfo> chunksContiguousRanges,
                                                                   @InOut(throwIfNull=true) InOutPrm<Integer> currentMergeIndex,
                                                                   @FlowFactory(flowType=MergeChunkSubRangeFlow.class) FlowFactoryPrm<MergeChunkSubRangeFlow> flowFactory,
                                                                   @In File outputFolder,
                                                                   @In HeliumEventNotifier heliumEventNotifier,
                                                                   @StepRef Transition LAUNCH_MERGE_PROCESSES,
                                                                   @StepRef Transition ZIP_BAD_CHUNKS) {
    int mergeIndex = currentMergeIndex.getInValue();
    if (mergeIndex < chunksContiguousRanges.size()) {
        ChunkRangeInfo chunkRangeInfo = chunksContiguousRanges.get(mergeIndex);
        MergeChunkSubRangeFlow mergeChunkSubRangeFlow = new MergeChunkSubRangeFlow(cameraName, chunkRangeInfo.chunkRange,
                chunkRangeInfo.endOfRange, outputFolder, heliumEventNotifier);

        FlowFuture<MergeChunkSubRangeFlow> flowFuture = flowFactory.runChildFlow(mergeChunkSubRangeFlow);

        return Futures.transform(flowFuture.getFuture(),
                ignored_ -> {
                    currentMergeIndex.setOutValue(mergeIndex + 1);
                    return LAUNCH_MERGE_PROCESSES;
                },
                MoreExecutors.directExecutor());
    } else {
        currentMergeIndex.setOutValue(mergeIndex);
        return Futures.immediateFuture(ZIP_BAD_CHUNKS);
    }
  }

    @SimpleStepFunction
    public static Transition ZIP_BAD_CHUNKS(@In Set<ChunkInfo> badChunks,
                                            @In File outputFolder,
                                            @In String cameraName,
                                            @In List<ChunkInfo> chunksToMerge,
                                            @In HeliumEventNotifier heliumEventNotifier,
                                            @StepRef Transition DELETE_MERGED_CHUNKS) throws IOException {
        // TODO: zip error reporting / handling?
        if (!badChunks.isEmpty()) {
          ChunkInfo firstChunk = chunksToMerge.get(0);
          ChunkInfo lastChunk = chunksToMerge.get(chunksToMerge.size() - 1);

          LocalDateTime startOfRange = getChunkDateTime(firstChunk.chunkFile.getName());
          LocalDateTime endOfRange = getChunkDateTime(lastChunk.chunkFile.getName());
          if (lastChunk.chunkDuration != null) {
              endOfRange = endOfRange.plus(secondsAsDoubleToDuration(lastChunk.chunkDuration));
          }

          File outputZipFile =
              new File(
                  outputFolder,
                  String.format(
                      "%s_merged_%s_%s_bad.zip",
                      cameraName,
                      ZIP_FILE_DATE_TIME_FORMATTER.format(startOfRange),
                      ZIP_FILE_DATE_TIME_FORMATTER.format(endOfRange)));

          if (outputZipFile.exists()) {
              File renameFilename = getRenameFileName(outputZipFile);

              // Pre-existing output file about to be renamed, throw event
              String eventTitleRename = String.format("Renaming pre-existing merge output file: [%s], renaming to [%s]",
                      outputZipFile.getAbsolutePath(), renameFilename.getAbsolutePath());
              heliumEventNotifier.notifyEvent(HELIUM_INGESTOR, HeliumEventType.FOUND_PRE_EXISTING_MERGE_OUTPUT_FILE, cameraName,
                      eventTitleRename, eventTitleRename);

              outputZipFile.renameTo(renameFilename);
          }

          zipFiles(badChunks.stream().map(c -> c.chunkFile).toList(), outputZipFile);
        }

        return DELETE_MERGED_CHUNKS;
    }

    @SimpleStepFunction
    public static Transition DELETE_MERGED_CHUNKS(@In List<ChunkInfo> chunksToMerge,
                                                  @Terminal Transition END) {
        //TODO: cleanup error reporting / handling?
        chunksToMerge.forEach(c -> c.chunkFile.delete());
        return END;
    }

    // ----------------------------------------------------------------------------

    static void notifyGapAndSurplusReport(
            String cameraName,
            List<ChunkInfo> currentRange,
            ChunkInfo chunkInfoAfterGap,
            LocalDateTime currentRangeWm,
            Duration distance,
            HeliumEventNotifier heliumEventNotifier,
            Duration actualDuration
    ) {
        // Gap inside the merge range found. Reporting event: GAP_IN_FOOTAGE
        String eventTitle = String.format("Gap found in footage starting from [%s] to [%s]. WM shows: [%s]. Distance [%s]",
                currentRange.get(0).chunkFile.getName(), chunkInfoAfterGap.chunkFile.getName(), currentRangeWm, distance);

        ChunkInfo lastChunk = currentRange.get(currentRange.size()-1);
        String durationMismatchReport = String.format("%s [%s]", lastChunk.chunkFile.getName(), lastChunk.chunkDuration);
        String eventDetails = String.format("%s%nDurations: [%s]", eventTitle, durationMismatchReport);

        long wmUnixTime = toUnixTime(currentRangeWm);
        long chunkUnixTime = getChunkUnixTime(chunkInfoAfterGap.chunkFile.getName());
        heliumEventNotifier.notifyEvent(System.currentTimeMillis(), HELIUM_INGESTOR, wmUnixTime, chunkUnixTime, HeliumEventType.GAP_IN_FOOTAGE, cameraName, eventTitle, eventDetails);

        Duration rangeDuration = getRangeDuration(currentRange, lastChunk).plus(secondsAsDoubleToDuration(checkNotNull(lastChunk.chunkDuration)));
        notifySurplusReport(cameraName, currentRange, chunkInfoAfterGap, heliumEventNotifier, rangeDuration, actualDuration);
    }

    static void notifySurplusReport(
            String cameraName,
            List<ChunkInfo> currentRange,
            ChunkInfo firstChunkOfTheNextRange,
            HeliumEventNotifier heliumEventNotifier,
            Duration rangeDuration,
            Duration actualDuration
    ) {
        Duration durationGap = rangeDuration.minus(actualDuration);
        if (durationGap.toMillis() > 0) {
            // Gap inside the merge range found. Reporting event:
            String eventTitle = String.format("Merged video starting from [%s] to [%s] shows extra footage duration of [%d] ms.",
                    currentRange.get(0).chunkFile.getName(), firstChunkOfTheNextRange.chunkFile.getName(), durationGap.toMillis());
            StringBuilder chunkDurationsReport = new StringBuilder();
            currentRange.forEach(chunk -> {
                if (!chunkDurationsReport.isEmpty()) {
                    chunkDurationsReport.append(" | ");
                }
                chunkDurationsReport.append(String.format("%s [%s]", chunk.chunkFile.getName(), chunk.chunkDuration));
            });
            String eventDetails = String.format("%s RangeDuration [%s] ActualDuration [%s]%nDurations: [%s]",
                    eventTitle, rangeDuration, actualDuration, chunkDurationsReport);
            long chunkUnixTime = getChunkUnixTime(firstChunkOfTheNextRange.chunkFile.getName());

            //TODO: event removed due to too many false positives, restore/debug
            LOGGER.warn(eventDetails);
            //heliumEventNotifier.notifyEvent(chunkUnixTime, HeliumEventType.SURPLUS_IN_FOOTAGE, cameraName, eventTitle, eventDetails);
        }
    }

    static Duration secondsAsDoubleToDuration(double seconds) {
        long wholeSeconds = (long)seconds;
        long nanoSeconds = (long)((seconds - wholeSeconds) * 1_000_000_000L);
        return Duration.ofSeconds(wholeSeconds, nanoSeconds);
    }

    static Duration getRangeDuration(List<ChunkInfo> currentRange,
                                     ChunkInfo firstChunkOfTheNextRange) {
        LocalDateTime firstChunkStart = getChunkDateTime(currentRange.get(0).chunkFile.getName());
        LocalDateTime nextRangeStart = getChunkDateTime(firstChunkOfTheNextRange.chunkFile.getName());
        return Duration.between(firstChunkStart, nextRangeStart);
    }

    static void zipFiles(Collection<File> filesToZip, File zipToFile) throws IOException {
        FileOutputStream fos = new FileOutputStream(zipToFile);
        ZipOutputStream zos = new ZipOutputStream(fos);

        for (File file : filesToZip) {
            if (file.exists()) {
                ZipEntry zipEntry = new ZipEntry(file.getName());
                zos.putNextEntry(zipEntry);

                FileInputStream fis = new FileInputStream(file);
                byte[] buffer = new byte[1024];
                int length;

                while ((length = fis.read(buffer)) > 0) {
                    zos.write(buffer, 0, length);
                }
                fis.close();
            }
        }
        zos.close();
    }
}
