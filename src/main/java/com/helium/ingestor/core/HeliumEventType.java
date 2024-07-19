package com.helium.ingestor.core;

public enum HeliumEventType {
  /** Helium Ingestor process events (java) */
  HELIUM_INGESTOR_PROCESS,
  /** Camera ffmpeg process started */
  CAMERA_PROCESS_STARTED,
  /** Camera ffmpeg process exited */
  CAMERA_PROCESS_TERMINATED,
  /** Underlying ffmpeg process became unresponsive, killed by camera process runner flow */
  CAMERA_PROCESS_FORCIBLY_KILLED,
  /** Failed to read chunk video file contents (e.g. to determine chunk duration) */
  VIDEO_CHUNK_NOT_READABLE,
  /** Chunk video file size change detected at merge */
  VIDEO_CHUNK_FILE_SIZE_MISMATCH,
  /** Chunk video file checksum change detected at merge */
  VIDEO_CHUNK_CHECKSUM_MISMATCH,
  /** Time gap discovered in video footage (e.g. at merging stage) */
  GAP_IN_FOOTAGE,
  /** Video footage for a period is longer than a given period. (hypothetical) */
  SURPLUS_IN_FOOTAGE,
  /** Attempt to merge video chunks failed */
  VIDEO_MERGING_FAILED,
  /** Found that Merge output file already existed, renamed old file */
  FOUND_PRE_EXISTING_MERGE_OUTPUT_FILE,
  /** Can't access archive service. Only fires if archive is enabled.
   TODO: we probably want to delete this event if we eventually succeed at archiving the file.
   TODO: maybe more like tombstone - can be ignored if followed by ARCHIVAL_SUCCEEDED.
   TODO: do we want to avoid event spam for every successful archival? */
  ARCHIVAL_FAILED_ATTEMPT,
  ARCHIVAL_SUCCEEDED,
  /** Can't reach archive service and deleted video due to out of space. Only fires if archive is enabled. */
  VIDEO_DELETED_DUE_TO_ARCHIVE_UNREACHABLE,
  /** Can't access frame analyzer service. Only fires if analyzer is enabled.
   TODO: we probably want to delete this event if we eventually succeed at reaching out to analyzer.
   TODO: maybe more like tombstone - can be ignored if followed by ANALYSIS_SUCCEEDED. */
  ANALYSIS_FAILED_ATTEMPT,
  /** Frame analyzed, list of objects detected, extracted frame with marks available */
  ANALYSIS_SUCCEEDED,
  /** Frame analyzed, list of changes in detected objects since last detection
   * It's possible to calculate on Client, but then report for a period will require a full scan every time. */
  ANALYSIS_RESULTS_DIFF,
  /** One of the Flower worker flows failed with Exception (e.g.: CameraProcessRunnerFlow, VideoChunkManagerFlow,
   * etc.) */
  FLOW_EXCEPTION,
  /** One of the Flower worker flows shuts down normally */
  FLOW_SHUTDOWN,
  /** Post-merge duration check failed */
  MERGED_FOOTAGE_DURATION_MISMATCH,
  /** Post-merge channel integrity check failed */
  MERGED_FOOTAGE_MISSING_MEDIA_CHANNELS
}
