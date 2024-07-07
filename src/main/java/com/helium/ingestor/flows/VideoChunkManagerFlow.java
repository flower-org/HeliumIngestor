package com.helium.ingestor.flows;

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
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

import com.flower.utilities.FuturesTool;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.helium.ingestor.core.HeliumEventNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FlowType(firstStep = "START_WATCHER")
public class VideoChunkManagerFlow {
    final static Logger LOGGER = LoggerFactory.getLogger(VideoChunkManagerFlow.class);
    final static String ARCHIVE_FOLDER_NAME = "merged";

    enum ChunkState {
        DISCOVERED,
        DURATION_LOADED,
        DURATION_LOAD_FAILED,
        MERGED,
        UNMERGEABLE
    }

    public static class ChunkInfo {
        ChunkState chunkState = ChunkState.DISCOVERED;
        @Nullable Double chunkDuration = null;
        @Nullable Exception durationLoadException = null;
    }

    @State final String cameraName;
    @State final HeliumEventNotifier heliumEventNotifier;
    @State @Nullable WatchService watcher;
    @State final File directoryFile;
    //TODO: filepath instead of path?
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
                                                    @StepRef Transition ADD_NEW_FILES_FROM_WATCHER_TO_TREE) {
        File[] files = directoryFile.listFiles(File::isFile);
        if (files != null) {
            for (File f : files) {
                if (!chunkInfoTreeMap.containsKey(f)) {
                    chunkInfoTreeMap.put(f, new ChunkInfo());
                }
            }
        }

        return ADD_NEW_FILES_FROM_WATCHER_TO_TREE;
    }

    @SimpleStepFunction
    public static Transition ADD_NEW_FILES_FROM_WATCHER_TO_TREE(@In File directoryFile,
                                                                @In TreeMap<File, ChunkInfo> chunkInfoTreeMap,
                                                                @In WatchService watcher,
                                                                @StepRef Transition LOAD_DURATIONS_FROM_CHUNK_FILES) {
        WatchKey watchKey = watcher.poll();
        if (watchKey != null) {
            List<WatchEvent<?>> events = watchKey.pollEvents();

            for (WatchEvent<?> event : events) {
                WatchEvent<Path> ev = (WatchEvent<Path>)event;
                File f = ev.context().toFile();
                if (!chunkInfoTreeMap.containsKey(f)) {
                    //For some reason here I'm getting file with the correct filename but incorrect parent directory
                    // so f.getAbsolutePath() returns non-existent path. The line below is to fix that.
                    f = new File(directoryFile, f.getName());
                    chunkInfoTreeMap.put(f, new ChunkInfo());
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
            @StepRef Transition ATTEMPT_TO_MERGE_CHUNKS) {
    // don't try to load LAST chunk's time because chances are that it's still downloading
    if (!chunkInfoTreeMap.isEmpty()) {
      File lastChunk = chunkInfoTreeMap.lastKey();
      //TODO: this cycle can be optimized but I'm too overwhelmed today with other stuff
      for (Map.Entry<File, ChunkInfo> entry : chunkInfoTreeMap.entrySet()) {
        File chunkFile = entry.getKey();
        ChunkInfo chunkInfo = entry.getValue();
        if (chunkFile != lastChunk) {
          if (chunkInfo.chunkState == ChunkState.DISCOVERED) {
            // Load video chunk duration.
            LoadChunkDurationFlow flow =
                new LoadChunkDurationFlow(
                    cameraName, chunkFile.getAbsolutePath(), heliumEventNotifier);
            FlowFuture<LoadChunkDurationFlow> loadDurationFlowFuture =
                loadChunkDurationFlowFactory.runChildFlow(flow);

            return FuturesTool.tryCatch(
                loadDurationFlowFuture.getFuture(),
                flowRet -> {
                  chunkInfo.chunkState = ChunkState.DURATION_LOADED;
                  chunkInfo.chunkDuration = flowRet.durationSeconds;
                  return LOAD_DURATIONS_FROM_CHUNK_FILES.setDelay(Duration.ofMillis(15));
                },
                Exception.class,
                e -> {
                  chunkInfo.chunkState = ChunkState.DURATION_LOAD_FAILED;
                  chunkInfo.durationLoadException = e;
                  return LOAD_DURATIONS_FROM_CHUNK_FILES.setDelay(Duration.ofMillis(15));
                },
                MoreExecutors.directExecutor());
          }
        }
      }
        }

        // No more chunks in state `DISCOVERED`
        return Futures.immediateFuture(ATTEMPT_TO_MERGE_CHUNKS.setDelay(Duration.ofMillis(1000L)));
    }

    @SimpleStepFunction
    public static Transition ATTEMPT_TO_MERGE_CHUNKS(@In TreeMap<File, ChunkInfo> chunkInfoTreeMap,
                                                     @StepRef Transition ADD_NEW_FILES_FROM_WATCHER_TO_TREE) {
        //chunkInfoTreeMap.lastKey()
        return ADD_NEW_FILES_FROM_WATCHER_TO_TREE;
    }
}
