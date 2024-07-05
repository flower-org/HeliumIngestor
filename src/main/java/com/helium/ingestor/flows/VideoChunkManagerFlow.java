package com.helium.ingestor.flows;

import com.flower.anno.event.EventProfiles;
import com.flower.anno.flow.FlowType;
import com.flower.anno.flow.State;
import com.flower.anno.functions.SimpleStepFunction;
import com.flower.anno.params.common.In;
import com.flower.anno.params.common.Out;
import com.flower.anno.params.transit.StepRef;
import com.flower.conf.OutPrm;
import com.flower.conf.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.TreeMap;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

@FlowType(firstStep = "START_WATCHER")
@EventProfiles({FlowTerminationEvents.class})
public class VideoChunkManagerFlow {
    final static Logger LOGGER = LoggerFactory.getLogger(VideoChunkManagerFlow.class);
    final static String ARCHIVE_FOLDER_NAME = "merged";

    enum ChunkState {
        DISCOVERED,
        DURATION_LOADED,
        MERGED,
        UNMERGEABLE
    }

    static class ChunkInfo {
        ChunkState chunkState = ChunkState.DISCOVERED;
        double chunkDuration = -1;
    }

    @State @Nullable WatchService watcher;
    @State final File directoryFile;
    @State final TreeMap<File, ChunkInfo> chunkInfoTreeMap;
    @State @Nullable File archiveDirectoryFile;

    public VideoChunkManagerFlow(File directoryFile) {
        this.directoryFile = directoryFile;
        chunkInfoTreeMap = new TreeMap<>();
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
                                                    @StepRef Transition GET_NEW_FILES_FROM_WATCHER) {
        File[] files = directoryFile.listFiles(File::isFile);
        if (files != null) {
            for (File f : files) {
                if (!chunkInfoTreeMap.containsKey(f)) {
                    chunkInfoTreeMap.put(f, new ChunkInfo());
                }
            }
        }

        return GET_NEW_FILES_FROM_WATCHER;
    }

    @SimpleStepFunction
    public static Transition GET_NEW_FILES_FROM_WATCHER(@In File directoryFile,
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
                    chunkInfoTreeMap.put(f, new ChunkInfo());
                }
            }
            watchKey.reset();
        }
        return LOAD_DURATIONS_FROM_CHUNK_FILES;
    }

    @SimpleStepFunction
    public static Transition LOAD_DURATIONS_FROM_CHUNK_FILES(@In File directoryFile,
                                                             @In TreeMap<File, ChunkInfo> chunkInfoTreeMap,
                                                             @StepRef Transition ATTEMPT_TO_MERGE_CHUNKS) {
        //TODO: don't try to load last chunk's time bc it's in progress

        //

        return ATTEMPT_TO_MERGE_CHUNKS;
    }

    @SimpleStepFunction
    public static Transition ATTEMPT_TO_MERGE_CHUNKS(@In File directoryFile,
                                                     @StepRef Transition GET_NEW_FILES_FROM_WATCHER) {
        return GET_NEW_FILES_FROM_WATCHER;
    }
}
