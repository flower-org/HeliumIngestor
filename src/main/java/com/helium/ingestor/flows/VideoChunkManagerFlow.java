package com.helium.ingestor.flows;

import static com.helium.ingestor.flows.LoadChunkDurationFlow.getChunkDateTime;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.step.FlowFactory;
import com.flower.anno.params.transit.StepRef;
import com.flower.conf.FlowFactoryPrm;
import com.flower.conf.FlowFuture;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.Adler32;
import javax.annotation.Nullable;

import com.flower.utilities.FuturesTool;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.helium.ingestor.core.HeliumEventNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: we determine gaps in footage post-factum, at merge stage
// The result of that is that our gap in footage event is delayed
// We can be more proactive about detecting gaps and throw events as soon as we determine the duration of chunks.
// P.S. Also, we can run range merge right away on detection of such gaps
@FlowType(firstStep = "START_WATCHER")
public class VideoChunkManagerFlow {
    final static Logger LOGGER = LoggerFactory.getLogger(VideoChunkManagerFlow.class);
    final static String ARCHIVE_FOLDER_NAME = "merged";

    enum ChunkState {
        DISCOVERED,
        DURATION_LOADED,
        DURATION_LOAD_FAILED,
        MERGED,
        ZIPPED_IN_A_BAD_CHUNK_ARCHIVE
    }

    public static class ChunkInfo {
        final File chunkFile;

        ChunkState chunkState;
        @Nullable Double chunkDuration = null;
        @Nullable Throwable durationLoadException = null;
        @Nullable Long fileLength = null;
        @Nullable byte[] checksum = null;

        public ChunkInfo(File chunkFile) {
            this.chunkState = ChunkState.DISCOVERED;
            this.chunkFile = chunkFile;
        }
    }

    @State final String cameraName;
    @State final HeliumEventNotifier heliumEventNotifier;
    @State @Nullable WatchService watcher;
    @State final File directoryFile;
    @State final TreeMap<File, ChunkInfo> chunkInfoTreeMap;
    @State final TreeMap<File, ChunkInfo> mergedChunkInfoTreeMap;
    @State @Nullable File archiveDirectoryFile;

    public VideoChunkManagerFlow(File directoryFile, String cameraName, HeliumEventNotifier heliumEventNotifier) {
        this.directoryFile = directoryFile;
        chunkInfoTreeMap = new TreeMap<>();
        mergedChunkInfoTreeMap = new TreeMap<>();

        this.cameraName = cameraName;
        this.heliumEventNotifier = heliumEventNotifier;
    }

    @SimpleStepFunction
    public static Transition START_WATCHER(@StepRef Transition INITIAL_DIRECTORY_LOAD,
                                           @Out OutPrm<WatchService> watcher,
                                           @In File directoryFile,
                                           @Out OutPrm<File> archiveDirectoryFile
    ) throws IOException {
        LOGGER.info("Starting directory watcher: {}", directoryFile);
        //Try creating directories
        if (!directoryFile.exists()) {
            try { directoryFile.mkdirs(); } catch (Exception e) {}
        }
        File archiveDirectoryFileVal = new File(directoryFile, ARCHIVE_FOLDER_NAME);
        if (!archiveDirectoryFileVal.exists()) {
            try { archiveDirectoryFileVal.mkdirs(); } catch (Exception e) {}
        }

        Path directoryPath = directoryFile.toPath();
        WatchService watcherVal = FileSystems.getDefault().newWatchService();
        directoryPath.register(watcherVal, ENTRY_CREATE);

        //Set out parameters
        watcher.setOutValue(watcherVal);
        archiveDirectoryFile.setOutValue(archiveDirectoryFileVal);

        return INITIAL_DIRECTORY_LOAD;
    }

    @SimpleStepFunction
    public static Transition INITIAL_DIRECTORY_LOAD(@In File directoryFile,
                                                    @In TreeMap<File, ChunkInfo> chunkInfoTreeMap,
                                                    @StepRef Transition ADD_NEW_FILES_FROM_WATCHER_TO_TREE) throws NoSuchAlgorithmException, IOException {
        File[] files = directoryFile.listFiles(File::isFile);
        if (files != null) {
            for (File chunkFile : files) {
                if (!chunkInfoTreeMap.containsKey(chunkFile)) {
                    chunkInfoTreeMap.put(chunkFile, new ChunkInfo(chunkFile));
                }
            }
        }

        return ADD_NEW_FILES_FROM_WATCHER_TO_TREE;
    }

    @SimpleStepFunction
    public static Transition ADD_NEW_FILES_FROM_WATCHER_TO_TREE(@In File directoryFile,
                                                                @In TreeMap<File, ChunkInfo> chunkInfoTreeMap,
                                                                @In WatchService watcher,
                                                                @StepRef Transition LOAD_DURATIONS_FROM_CHUNK_FILES) throws NoSuchAlgorithmException, IOException {
        WatchKey watchKey = watcher.poll();
        if (watchKey != null) {
            List<WatchEvent<?>> events = watchKey.pollEvents();

            for (WatchEvent<?> event : events) {
                WatchEvent<Path> ev = (WatchEvent<Path>)event;
                File chunkFile = ev.context().toFile();
                if (!chunkInfoTreeMap.containsKey(chunkFile)) {
                    //For some reason here I'm getting file with the correct filename but incorrect parent directory
                    // so f.getAbsolutePath() returns non-existent path. The line below is to fix that.
                    chunkFile = new File(directoryFile, chunkFile.getName());
                    chunkInfoTreeMap.put(chunkFile, new ChunkInfo(chunkFile));
                }
            }
            watchKey.reset();
        }
        return LOAD_DURATIONS_FROM_CHUNK_FILES;
    }

    @SimpleStepFunction
    public static ListenableFuture<Transition> LOAD_DURATIONS_FROM_CHUNK_FILES(
            @In String cameraName,
            @In HeliumEventNotifier heliumEventNotifier,
            @In TreeMap<File, ChunkInfo> chunkInfoTreeMap,
            @FlowFactory(flowType=LoadChunkDurationFlow.class) FlowFactoryPrm<LoadChunkDurationFlow> loadChunkDurationFlowFactory,
            @StepRef Transition LOAD_DURATIONS_FROM_CHUNK_FILES,
            @StepRef Transition ATTEMPT_TO_MERGE_CHUNKS,
            @StepRef Transition ADD_NEW_FILES_FROM_WATCHER_TO_TREE) {
        // don't try to load LAST chunk's time because chances are that it's still downloading
        if (!chunkInfoTreeMap.isEmpty()) {
          File lastChunk = chunkInfoTreeMap.lastKey();
          //TODO: this cycle can be optimized but I'm too overwhelmed today with other stuff
          ChunkInfo previousChunk = null;
          for (Map.Entry<File, ChunkInfo> entry : chunkInfoTreeMap.entrySet()) {
            ChunkInfo currentChunkInfo = entry.getValue();
            if (previousChunk == null) {
                previousChunk = currentChunkInfo;
            } else {
                File chunkFile = previousChunk.chunkFile;
                if (chunkFile != lastChunk) {
                    if (previousChunk.chunkState == ChunkState.DISCOVERED) {
                        // Load video chunk duration.
                        ChunkInfo chunkInfo = previousChunk;
                        Duration expectedDuration = Duration.between(getChunkDateTime(previousChunk.chunkFile.getName()), getChunkDateTime(currentChunkInfo.chunkFile.getName()));
                        if (expectedDuration.toMillis() <= 2000) {
                            //Don't load durations for chunks <= 2s
                            chunkInfo.chunkState = ChunkState.DURATION_LOADED;
                            chunkInfo.chunkDuration = expectedDuration.toMillis() / 1_000D;
                            chunkInfo.fileLength = chunkFile.length();
                        } else {
                            //TODO: instead of DurationFlow/ffprobe write a lightweight function to extract duration from mp4
                            // Ideally pair that task with sending the chunk to the Analyzer service.
                            LoadChunkDurationFlow flow =
                                    new LoadChunkDurationFlow(cameraName, chunkFile.getAbsolutePath(), heliumEventNotifier);
                            FlowFuture<LoadChunkDurationFlow> loadDurationFlowFuture =
                                    loadChunkDurationFlowFactory.runChildFlow(flow);

                            return FuturesTool.tryCatch(
                                    loadDurationFlowFuture.getFuture(),
                                    flowRet -> {
                                        if (chunkInfo.durationLoadException == null) {
                                            chunkInfo.chunkState = ChunkState.DURATION_LOADED;
                                            chunkInfo.chunkDuration = flowRet.durationSeconds;
                                            chunkInfo.fileLength = chunkFile.length();
                                            //chunkInfo.checksum = getChecksumForFile(chunkFile);

                                            return ATTEMPT_TO_MERGE_CHUNKS.setDelay(Duration.ofMillis(1));
                                        } else {
                                            chunkInfo.chunkState = ChunkState.DURATION_LOAD_FAILED;
                                            chunkInfo.durationLoadException = flowRet.durationException;
                                            return LOAD_DURATIONS_FROM_CHUNK_FILES.setDelay(Duration.ofMillis(1));
                                        }

                                    },
                                    Exception.class,
                                    e -> {
                                        chunkInfo.chunkState = ChunkState.DURATION_LOAD_FAILED;
                                        chunkInfo.durationLoadException = e;
                                        return LOAD_DURATIONS_FROM_CHUNK_FILES.setDelay(Duration.ofMillis(1));
                                    },
                                    MoreExecutors.directExecutor());
                        }
                    }
                }
                previousChunk = currentChunkInfo;
            }
          }

            // No more chunks in state `DISCOVERED`
            return Futures.immediateFuture(ATTEMPT_TO_MERGE_CHUNKS.setDelay(Duration.ofMillis(1000L)));
        } else {
            // No chunks in the tree, need to load chunks
            return Futures.immediateFuture(ADD_NEW_FILES_FROM_WATCHER_TO_TREE.setDelay(Duration.ofMillis(1000L)));
        }
    }

    @SimpleStepFunction
    public static ListenableFuture<Transition> ATTEMPT_TO_MERGE_CHUNKS(@In String cameraName,
                                                     @In TreeMap<File, ChunkInfo> chunkInfoTreeMap,
                                                     @In File directoryFile,
                                                     @In HeliumEventNotifier heliumEventNotifier,
                                                     @FlowFactory(flowType = AnalyzeAndMergeChunkRangeFlow.class) FlowFactoryPrm<AnalyzeAndMergeChunkRangeFlow> flowFactory,
                                                     @StepRef Transition ADD_NEW_FILES_FROM_WATCHER_TO_TREE) {
        File firstChunk = chunkInfoTreeMap.firstKey();
        File lastChunk = chunkInfoTreeMap.lastKey();

        //1. If firstChunk is different hour from lastChunk, we can merge oldest hour.
        LocalDateTime firstChunkDateTime = getChunkDateTime(firstChunk.getName());
        LocalDateTime lastChunkDateTime = getChunkDateTime(lastChunk.getName());

        boolean hourOrMoreBefore = firstChunkDateTime.isBefore(lastChunkDateTime.minusHours(1L));
        boolean differentHour = firstChunkDateTime.getHour() != lastChunkDateTime.getHour();
        if (!hourOrMoreBefore && !differentHour) {
            //can't merge yet
            return Futures.immediateFuture(ADD_NEW_FILES_FROM_WATCHER_TO_TREE);
        }

        //2. If there are no chunks with DISCOVERED state in the first hour, we can merge.
        for (Map.Entry<File, ChunkInfo> chunkInfoEntry : chunkInfoTreeMap.entrySet()) {
            File chunkFile = chunkInfoEntry.getKey();
            ChunkInfo chunkInfo = chunkInfoEntry.getValue();

            if (chunkInfo.chunkState == ChunkState.DISCOVERED) {
                //can't merge yet
                return Futures.immediateFuture(ADD_NEW_FILES_FROM_WATCHER_TO_TREE);
            }

            LocalDateTime chunkDateTime = getChunkDateTime(chunkFile.getName());
            if (!hourMatches(chunkDateTime, firstChunkDateTime)) {
                //Iterated through all chunks in merge hour
                break;
            }
        }

        //3. Analyze list of chunks to merge
        List<ChunkInfo> chunksToMerge = new ArrayList<>();
        while (!chunkInfoTreeMap.isEmpty()) {
            LocalDateTime chunkDateTime = getChunkDateTime(chunkInfoTreeMap.firstEntry().getKey().getName());
            if (!hourMatches(chunkDateTime, firstChunkDateTime)) { break; }

            chunksToMerge.add(chunkInfoTreeMap.pollFirstEntry().getValue());
        }

        //4. run flow to merge the final range
        File outputFolder = new File(directoryFile, "merged");
        AnalyzeAndMergeChunkRangeFlow analyzeAndMergeChunkRangeFlow = new AnalyzeAndMergeChunkRangeFlow(
                cameraName, outputFolder, heliumEventNotifier,
                chunksToMerge, chunkInfoTreeMap.firstEntry().getValue());
        FlowFuture<AnalyzeAndMergeChunkRangeFlow> flowFuture = flowFactory.runChildFlow(analyzeAndMergeChunkRangeFlow);
        return Futures.transform(flowFuture.getFuture(),
            ignored_ -> ADD_NEW_FILES_FROM_WATCHER_TO_TREE.setDelay(Duration.ofSeconds(1)),
            MoreExecutors.directExecutor());
    }

    // --------------------------------------------------------------------------------------------

    static boolean hourMatches(LocalDateTime dateTime, LocalDateTime hourToMatch) {
        return (dateTime.getYear() == hourToMatch.getYear()
            && dateTime.getDayOfYear() == hourToMatch.getDayOfYear()
            && dateTime.getHour() == hourToMatch.getHour());
    }

    static byte[] getChecksumForFile(File file) {
        return getAdler32ForFile(file);
        //SHA-256 too slow
        //return getSha256ForFile(file);
    }

    static byte[] getSha256ForFile(File file) {
        try {
            // Use SHA-256 algorithm
            MessageDigest sha256Digest = MessageDigest.getInstance("SHA-256");

            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] byteArray = new byte[1024];
                int byteCount = 0;

                // Read file data and update digest
                while ((byteCount = fis.read(byteArray)) != -1) {
                    sha256Digest.update(byteArray, 0, byteCount);
                }
            }

            return sha256Digest.digest();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static byte[] getAdler32ForFile(File file) {
        try {
            Adler32 adler32 = new Adler32();

            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] byteArray = new byte[1024];
                int byteCount = 0;

                // Read file data and update digest
                while ((byteCount = fis.read(byteArray)) != -1) {
                    adler32.update(byteArray, 0, byteCount);
                }
            }

            long crc = adler32.getValue();
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(crc);
            return buffer.array();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String byteArrayToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }

        return sb.toString();
    }
}
