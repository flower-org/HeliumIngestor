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
  // TODO: what do? delete chunks? mark as unmergeable?
  VIDEO_MERGING_FAILED,
  /** Found that Merge output file already existed, renamed */
  FOUND_PRE_EXISTING_MERGE_OUTPUT_FILE,
  /** Can't access archive service */
  ARCHIVE_UNREACHABLE,
  /** Can't reach archive service and deleted video due to out of space */
  VIDEO_DELETED_DUE_TO_ARCHIVE_UNREACHABLE,
  /** Can't access frame analyzer service */
  ANALYZER_UNREACHABLE,
  /** Frame analyzed, list objects detected */
  ANALYSIS_RESULTS,
  /** Frame analyzed, list of changes in detected objects since last detection */
  ANALYSIS_RESULTS_DIFF,
  /** One of the Flower worker flows failed with Exception (e.g.: CameraProcessRunnerFlow, VideoChunkManagerFlow,
   * etc.) */
  FLOW_EXCEPTION,
  /** One of the Flower worker flows shuts down normally */
  FLOW_SHUTDOWN
}
