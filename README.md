export JAVA_HOME=/home/john/.jdks/graalvm-jdk-17.0.11

echo $JAVA_HOME

./gradlew shadowJar -Dorg.gradle.java.home=/home/john/.jdks/graalvm-jdk-17.0.11

- AnalyzeAndMergeChunkRangeFlow
```mermaid
graph
    BEGIN
    END
    INTEGRITY_CHECK
    BEGIN --> INTEGRITY_CHECK
    INTEGRITY_CHECK --> CONTINUITY_CHECK
    ZIP_BAD_CHUNKS
    ZIP_BAD_CHUNKS --> DELETE_MERGED_CHUNKS
    CONTINUITY_CHECK
    CONTINUITY_CHECK -- If contiguous duration ranges were determined successfully,
validate audio continuity for those ranges next --> AUDIO_CONTINUITY_CHECK
    CONTINUITY_CHECK -- If there are no ranges of valid chunks to merge,
go directly to zipping bad chunks step --> ZIP_BAD_CHUNKS
    AUDIO_CONTINUITY_CHECK
    AUDIO_CONTINUITY_CHECK --> LAUNCH_MERGE_PROCESSES
    DELETE_MERGED_CHUNKS
    DELETE_MERGED_CHUNKS --> END
    LAUNCH_MERGE_PROCESSES
    LAUNCH_MERGE_PROCESSES -- Merge one range at a time, in cycle --> LAUNCH_MERGE_PROCESSES
    LAUNCH_MERGE_PROCESSES -- Sometimes merge results in error
'Impossible to open' due to some chunk in the middle.
In such cases we mark that chunk as bad and re-start range creation --> INTEGRITY_CHECK
    LAUNCH_MERGE_PROCESSES -- Once all ranges are merged,
we proceed to zip bad chunks --> ZIP_BAD_CHUNKS
LAUNCH_MERGE_PROCESSES -. Run merge operation for a range in a child Flow .-> ChildFlow:MergeChunkSubRangeFlow

```
- CameraProcessRunnerFlow
```mermaid
graph
BEGIN
END
LAUNCH_PROCESS
BEGIN --> LAUNCH_PROCESS
LAUNCH_PROCESS --> LAUNCH_PROCESS
LAUNCH_PROCESS --> READ_PROCESS_OUTPUT
CHECK_PROCESS_STATE
CHECK_PROCESS_STATE --> READ_PROCESS_OUTPUT
CHECK_PROCESS_STATE --> LAUNCH_PROCESS
READ_PROCESS_OUTPUT
READ_PROCESS_OUTPUT --> READ_PROCESS_OUTPUT
READ_PROCESS_OUTPUT --> CHECK_PROCESS_STATE
```
- LoadVideoChannelsFlow
```mermaid
graph
BEGIN
END
LAUNCH_PROCESS
BEGIN --> LAUNCH_PROCESS
LAUNCH_PROCESS --> LAUNCH_PROCESS
LAUNCH_PROCESS --> READ_PROCESS_OUTPUT
LAUNCH_PROCESS --> END
READ_PROCESS_OUTPUT
READ_PROCESS_OUTPUT --> READ_PROCESS_OUTPUT
READ_PROCESS_OUTPUT --> PARSE_OUTPUT
PARSE_OUTPUT
PARSE_OUTPUT --> END
```
- LoadVideoDurationFlow
```mermaid
graph
BEGIN
END
LAUNCH_PROCESS
BEGIN --> LAUNCH_PROCESS
LAUNCH_PROCESS --> LAUNCH_PROCESS
LAUNCH_PROCESS --> READ_PROCESS_OUTPUT
LAUNCH_PROCESS --> END
READ_PROCESS_OUTPUT
READ_PROCESS_OUTPUT --> READ_PROCESS_OUTPUT
READ_PROCESS_OUTPUT --> PARSE_OUTPUT
PARSE_OUTPUT
PARSE_OUTPUT --> END
```
- MergeChunkSubRangeFlow
```mermaid
graph
BEGIN
END
LAUNCH_PROCESS
LAUNCH_PROCESS -- If max retries was exceeded, 
we terminate the flow --> END
LAUNCH_PROCESS -- If merge process started successfully,
we proceed to read its STDOUT/ERR --> READ_PROCESS_OUTPUT
LAUNCH_PROCESS -- If exception was encountered while starting
merge process, we retry up to 3 times --> LAUNCH_PROCESS
INIT_PROCESS
BEGIN --> INIT_PROCESS
INIT_PROCESS --> LAUNCH_PROCESS
READ_PROCESS_OUTPUT
READ_PROCESS_OUTPUT -- While the process is alive,
we keep reading its STDOUT/ERR --> READ_PROCESS_OUTPUT
READ_PROCESS_OUTPUT -- If process terminated, we proceed
to parse the collected output --> PARSE_OUTPUT
PARSE_OUTPUT
POST_MERGE_DURATION_CHECK
PARSE_OUTPUT -- If the merge process finished successfully,
run pos-merge checks--> POST_MERGE_DURATION_CHECK
POST_MERGE_AUDIO_VIDEO_INTEGRITY_CHECK
POST_MERGE_DURATION_CHECK --After duration check, check that both
audio and video channels are present--> POST_MERGE_AUDIO_VIDEO_INTEGRITY_CHECK
POST_MERGE_AUDIO_VIDEO_INTEGRITY_CHECK --> END

POST_MERGE_DURATION_CHECK -. Run `GetDuration` operation
for a video in a child Flow .-> ChildFlow:LoadVideoDurationFlow
POST_MERGE_AUDIO_VIDEO_INTEGRITY_CHECK -. Run `GetChannels` operation
for a video in a child Flow .-> ChildFlow:LoadMediaChannelsFlow
```

- MainIngestorFlow
```mermaid
graph
BEGIN
END
LOAD_CAMERAS_FROM_CONFIG
BEGIN --> LOAD_CAMERAS_FROM_CONFIG
LOAD_CAMERAS_FROM_CONFIG --> RUN_CHILD_FLOWS
FINALIZE
FINALIZE --> END
RUN_CHILD_FLOWS
RUN_CHILD_FLOWS --> FINALIZE
```
- VideoChunkManagerFlow
```mermaid
graph
BEGIN
END
LOAD_DURATIONS_FROM_CHUNK_FILES
LOAD_DURATIONS_FROM_CHUNK_FILES --> LOAD_DURATIONS_FROM_CHUNK_FILES
LOAD_DURATIONS_FROM_CHUNK_FILES --> ATTEMPT_TO_MERGE_CHUNKS
LOAD_DURATIONS_FROM_CHUNK_FILES --> ADD_NEW_FILES_FROM_WATCHER_TO_TREE
START_WATCHER
BEGIN --> START_WATCHER
START_WATCHER --> INITIAL_DIRECTORY_LOAD
INITIAL_DIRECTORY_LOAD
INITIAL_DIRECTORY_LOAD --> ADD_NEW_FILES_FROM_WATCHER_TO_TREE
ATTEMPT_TO_MERGE_CHUNKS
ATTEMPT_TO_MERGE_CHUNKS --> ADD_NEW_FILES_FROM_WATCHER_TO_TREE
ADD_NEW_FILES_FROM_WATCHER_TO_TREE
ADD_NEW_FILES_FROM_WATCHER_TO_TREE --> LOAD_DURATIONS_FROM_CHUNK_FILES
```
- VideoFootageArchiverFlow
```mermaid
graph
    BEGIN
    END
    LAUNCH_PROCESS
    BEGIN --> LAUNCH_PROCESS
    LAUNCH_PROCESS --> LAUNCH_PROCESS
    LAUNCH_PROCESS --> READ_PROCESS_OUTPUT
    TRY_ARCHIVE_FILE_AND_DELETE_IF_FAILS
    TRY_ARCHIVE_FILE_AND_DELETE_IF_FAILS --> LAUNCH_PROCESS
    READ_PROCESS_OUTPUT
    READ_PROCESS_OUTPUT --> READ_PROCESS_OUTPUT
    READ_PROCESS_OUTPUT --> PARSE_OUTPUT
    TRY_ARCHIVE_FILE_AND_RETRY_IF_FAILS
    TRY_ARCHIVE_FILE_AND_RETRY_IF_FAILS --> LAUNCH_PROCESS
    PARSE_OUTPUT
    PARSE_OUTPUT --> LAUNCH_PROCESS
    PARSE_OUTPUT --> TRY_ARCHIVE_FILE_AND_DELETE_IF_FAILS
    PARSE_OUTPUT --> TRY_ARCHIVE_FILE_AND_RETRY_IF_FAILS
```
