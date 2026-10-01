package com.example.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * JSON view of the simulated partition log returned by
 * {@code GET /api/v1/log-engine/inspect}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StorageSegmentView {

    private String topic;
    private int partition;
    private String partitionDirectory;
    private String generatedAt;
    private ConfigView config;

    private long logStartOffset;
    /** LEO: offset of the NEXT record the leader will append. */
    private long logEndOffset;
    /** HW: records below this offset are replicated to all ISR members and visible to consumers. */
    private long highWatermark;
    private long messagesAboveHighWatermark;

    private List<ReplicaView> replicas;
    private List<String> inSyncReplicas;
    private SegmentInfo activeSegment;
    private List<SegmentInfo> segments;
    private LookupTrace lookupTrace;
    private List<String> notes;

    public record ConfigView(int segmentBytes, int indexIntervalBytes, int maxFollowerLag) {
    }

    public record ReplicaView(String replicaId, String role, long logEndOffset,
                              long lagBehindLeader, boolean inSync, boolean paused) {
    }

    /** Sparse offset-index entry: (relative offset, physical byte position in the .log file). */
    public record IndexEntryView(int relativeOffset, long absoluteOffset, int filePosition) {
    }

    /** Time-index entry: (max timestamp so far, relative offset). */
    public record TimeIndexEntryView(long timestamp, String timestampIso,
                                     int relativeOffset, long absoluteOffset) {
    }

    public record RecordView(long offset, int position, int sizeBytes, long timestamp,
                             String key, String value, boolean crcValid,
                             boolean aboveHighWatermark) {

        public RecordView markVisibility(long highWatermark) {
            return new RecordView(offset, position, sizeBytes, timestamp, key, value, crcValid,
                    offset >= highWatermark);
        }

        public RecordView abbreviated(int maxValueChars) {
            String v = value;
            if (v != null && v.length() > maxValueChars) {
                v = v.substring(0, maxValueChars) + "...";
            }
            return new RecordView(offset, position, sizeBytes, timestamp, key, v, crcValid,
                    aboveHighWatermark);
        }
    }

    /** Step-by-step explanation of how an offset is located: segment -> sparse index -> linear scan. */
    public record LookupTrace(long targetOffset, boolean found, Long segmentBaseOffset,
                              String segmentLogFile, Integer relativeOffset,
                              IndexEntryView indexEntryUsed, int indexBinarySearchSteps,
                              long scanStartPosition, int recordsScanned, int bytesScanned,
                              Integer recordPosition, String explanation) {
    }

    public record SegmentInfo(String logFile, String indexFile, String timeIndexFile,
                              long baseOffset, long nextOffset, boolean active,
                              int recordCount, long logSizeBytes, long indexSizeBytes,
                              long timeIndexSizeBytes, long largestTimestamp,
                              List<IndexEntryView> indexEntries,
                              List<TimeIndexEntryView> timeIndexEntries,
                              List<RecordView> records, boolean recordsTruncated) {
    }
}
