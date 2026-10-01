package com.example.kafka.simulator;

import com.example.kafka.model.StorageSegmentView;
import com.example.kafka.model.StorageSegmentView.ConfigView;
import com.example.kafka.model.StorageSegmentView.IndexEntryView;
import com.example.kafka.model.StorageSegmentView.LookupTrace;
import com.example.kafka.model.StorageSegmentView.RecordView;
import com.example.kafka.model.StorageSegmentView.ReplicaView;
import com.example.kafka.model.StorageSegmentView.SegmentInfo;
import com.example.kafka.model.StorageSegmentView.TimeIndexEntryView;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.zip.CRC32;

/**
 * Educational, disk-backed simulation of one Kafka partition's storage layer.
 *
 * <h3>On-disk layout (per segment, named by zero-padded base offset)</h3>
 * <pre>
 *   00000000000000000000.log        record data
 *   00000000000000000000.index      sparse offset index: [relativeOffset:int32][position:int32]
 *   00000000000000000000.timeindex  sparse time index:   [timestamp:int64][relativeOffset:int32]
 * </pre>
 *
 * <h3>Record format (simplified record batch of one record)</h3>
 * <pre>
 *   offset:int64 | size:int32 | crc32:int32 | timestamp:int64 | keyLen:int32 | key | valueLen:int32 | value
 * </pre>
 *
 * <h3>Replication model</h3>
 * One leader and two simulated followers. LEO = next offset the leader will assign. The High
 * Watermark is the minimum LEO across the in-sync replicas (ISR); only records below HW are readable
 * by consumers. A follower that falls more than {@code max-follower-lag} records behind leaves the ISR
 * (a simplification of Kafka's time-based replica.lag.time.max.ms).
 *
 * <p>State is reset at startup (no crash recovery); all public methods are synchronized.
 */
@Slf4j
@Component
public class KafkaStorageEngineSimulator {

    public static final List<String> FOLLOWER_IDS = List.of("follower-1", "follower-2");

    private static final int RECORD_HEADER_BYTES = 12;       // offset(8) + size(4)
    private static final int OFFSET_INDEX_ENTRY_BYTES = 8;   // relOffset(4) + position(4)
    private static final int TIME_INDEX_ENTRY_BYTES = 12;    // timestamp(8) + relOffset(4)

    private final Path partitionDir;
    private final String topic;
    private final int partition;
    private final int segmentBytes;
    private final int indexIntervalBytes;
    private final int maxFollowerLag;

    private final NavigableMap<Long, LogSegment> segments = new TreeMap<>();
    private final Map<String, Long> followerLogEndOffsets = new LinkedHashMap<>();
    private final Set<String> pausedFollowers = new HashSet<>();

    private LogSegment activeSegment;
    private long logStartOffset;
    private long logEndOffset;
    private long highWatermark;

    public KafkaStorageEngineSimulator(
            @Value("${app.log-engine.base-dir}") String baseDir,
            @Value("${app.log-engine.topic:orders}") String topic,
            @Value("${app.log-engine.partition:0}") int partition,
            @Value("${app.log-engine.segment-bytes:1024}") int segmentBytes,
            @Value("${app.log-engine.index-interval-bytes:256}") int indexIntervalBytes,
            @Value("${app.log-engine.max-follower-lag:5}") int maxFollowerLag) {
        this.topic = topic;
        this.partition = partition;
        this.segmentBytes = segmentBytes;
        this.indexIntervalBytes = indexIntervalBytes;
        this.maxFollowerLag = maxFollowerLag;
        this.partitionDir = Paths.get(baseDir).resolve(topic + "-" + partition);
    }

    // ------------------------------------------------------------------
    // Public result types
    // ------------------------------------------------------------------

    public record AppendResult(long offset, long segmentBaseOffset, int position, int sizeBytes,
                               long timestamp, boolean indexEntryAdded, boolean segmentRolled,
                               long logEndOffset, long highWatermark) {
    }

    public record ReadResult(long requestedOffset, long highWatermark, long logEndOffset,
                             List<RecordView> records, LookupTrace lookupTrace, String note) {
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @PostConstruct
    public synchronized void init() {
        resetInternal();
        log.info("Log engine simulator ready at {}", partitionDir);
    }

    @PreDestroy
    public synchronized void shutdown() {
        closeAllSegments();
    }

    public synchronized void reset() {
        closeAllSegments();
        resetInternal();
        log.info("Log engine simulator reset");
    }

    private void resetInternal() {
        try {
            Files.createDirectories(partitionDir);
            try (Stream<Path> files = Files.list(partitionDir)) {
                files.filter(Files::isRegularFile).forEach(this::deleteQuietly);
            }
            segments.clear();
            logStartOffset = 0;
            logEndOffset = 0;
            highWatermark = 0;
            pausedFollowers.clear();
            followerLogEndOffsets.clear();
            for (String id : FOLLOWER_IDS) {
                followerLogEndOffsets.put(id, 0L);
            }
            activeSegment = createSegment(0L);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot initialise log directory " + partitionDir, e);
        }
    }

    private void closeAllSegments() {
        for (LogSegment segment : segments.values()) {
            try {
                segment.close();
            } catch (IOException e) {
                log.warn("Failed closing segment {}: {}", segment.baseOffset, e.getMessage());
            }
        }
        segments.clear();
    }

    private void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.warn("Could not delete {}: {}", path, e.getMessage());
        }
    }

    private LogSegment createSegment(long baseOffset) throws IOException {
        LogSegment segment = new LogSegment(partitionDir, baseOffset);
        segments.put(baseOffset, segment);
        return segment;
    }

    // ------------------------------------------------------------------
    // Write path
    // ------------------------------------------------------------------

    /**
     * Appends a record to the active segment (rolling first if it would exceed segment.bytes).
     *
     * @param replicate if true, all non-paused followers immediately fetch up to the new LEO
     */
    public synchronized AppendResult append(String key, String value, boolean replicate) {
        try {
            boolean rolled = false;
            int encoded = LogSegment.encodedSize(key, value);
            if (activeSegment.sizeBytes > 0 && activeSegment.sizeBytes + encoded > segmentBytes) {
                rollInternal();
                rolled = true;
            }
            SegmentAppend appended = activeSegment.append(key, value, System.currentTimeMillis(),
                    indexIntervalBytes);
            logEndOffset = appended.offset() + 1;

            if (replicate) {
                replicateHealthyFollowers();
            } else {
                updateHighWatermark();
            }
            return new AppendResult(appended.offset(), activeSegment.baseOffset, appended.position(),
                    appended.totalBytes(), appended.timestamp(), appended.indexed(), rolled,
                    logEndOffset, highWatermark);
        } catch (IOException e) {
            throw new UncheckedIOException("Append failed", e);
        }
    }

    /** Forces the active segment to be sealed and a new one started at the current LEO. */
    public synchronized long roll() {
        try {
            rollInternal();
            return activeSegment.baseOffset;
        } catch (IOException e) {
            throw new UncheckedIOException("Roll failed", e);
        }
    }

    private void rollInternal() throws IOException {
        activeSegment.seal();
        activeSegment = createSegment(logEndOffset);
        log.info("Rolled new segment with base offset {}", logEndOffset);
    }

    // ------------------------------------------------------------------
    // Replication / high watermark
    // ------------------------------------------------------------------

    /** Simulates a follower fetch that advances its LEO by up to {@code count} records. */
    public synchronized Map<String, Object> replicate(String followerId, long count) {
        requireFollower(followerId);
        if (pausedFollowers.contains(followerId)) {
            throw new IllegalStateException(followerId + " is paused; resume it before replicating");
        }
        long current = followerLogEndOffsets.get(followerId);
        long target = Math.min(logEndOffset, current + Math.max(0, count));
        followerLogEndOffsets.put(followerId, target);
        updateHighWatermark();
        return replicationSummary();
    }

    public synchronized Map<String, Object> pauseFollower(String followerId) {
        requireFollower(followerId);
        pausedFollowers.add(followerId);
        updateHighWatermark();
        return replicationSummary();
    }

    public synchronized Map<String, Object> resumeFollower(String followerId) {
        requireFollower(followerId);
        pausedFollowers.remove(followerId);
        updateHighWatermark();
        return replicationSummary();
    }

    private void replicateHealthyFollowers() {
        for (String id : FOLLOWER_IDS) {
            if (!pausedFollowers.contains(id)) {
                followerLogEndOffsets.put(id, logEndOffset);
            }
        }
        updateHighWatermark();
    }

    /** HW = min(LEO of all in-sync replicas); it never moves backwards. */
    private void updateHighWatermark() {
        long min = logEndOffset;
        for (String id : FOLLOWER_IDS) {
            if (isInSync(id)) {
                min = Math.min(min, followerLogEndOffsets.get(id));
            }
        }
        if (min > highWatermark) {
            highWatermark = min;
        }
    }

    private boolean isInSync(String followerId) {
        return logEndOffset - followerLogEndOffsets.get(followerId) <= maxFollowerLag;
    }

    private void requireFollower(String followerId) {
        if (!followerLogEndOffsets.containsKey(followerId)) {
            throw new IllegalArgumentException("Unknown follower '" + followerId + "'; valid: " + FOLLOWER_IDS);
        }
    }

    private Map<String, Object> replicationSummary() {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("logEndOffset", logEndOffset);
        summary.put("highWatermark", highWatermark);
        summary.put("followerLogEndOffsets", new LinkedHashMap<>(followerLogEndOffsets));
        summary.put("pausedFollowers", new ArrayList<>(pausedFollowers));
        summary.put("inSyncReplicas", inSyncReplicaIds());
        return summary;
    }

    private List<String> inSyncReplicaIds() {
        List<String> isr = new ArrayList<>();
        isr.add("leader");
        for (String id : FOLLOWER_IDS) {
            if (isInSync(id)) {
                isr.add(id);
            }
        }
        return isr;
    }

    // ------------------------------------------------------------------
    // Read path
    // ------------------------------------------------------------------

    /** Consumer-style read: returns records from {@code fromOffset} but never at/above the HW. */
    public synchronized ReadResult read(long fromOffset, int maxRecords) {
        try {
            int limit = Math.max(1, Math.min(maxRecords, 500));
            LookupTrace trace = traceLookupInternal(fromOffset);
            List<RecordView> out = new ArrayList<>();
            String note;

            if (!trace.found()) {
                note = trace.explanation();
            } else if (fromOffset >= highWatermark) {
                note = "Offset " + fromOffset + " exists (LEO=" + logEndOffset + ") but is at/above the"
                        + " high watermark (" + highWatermark + "): not yet replicated to the ISR, so"
                        + " consumers cannot see it.";
            } else {
                LogSegment segment = segments.get(trace.segmentBaseOffset());
                long position = trace.recordPosition();
                while (segment != null && out.size() < limit) {
                    if (position >= segment.sizeBytes) {
                        Map.Entry<Long, LogSegment> next = segments.higherEntry(segment.baseOffset);
                        segment = next == null ? null : next.getValue();
                        position = 0;
                        continue;
                    }
                    RecordView rec = segment.readAt(position);
                    if (rec.offset() >= highWatermark) {
                        break;
                    }
                    out.add(rec.markVisibility(highWatermark));
                    position += rec.sizeBytes();
                }
                note = "Returned " + out.size() + " record(s), bounded by high watermark " + highWatermark + ".";
            }
            return new ReadResult(fromOffset, highWatermark, logEndOffset, out, trace, note);
        } catch (IOException e) {
            throw new UncheckedIOException("Read failed", e);
        }
    }

    public synchronized LookupTrace traceLookup(long targetOffset) {
        try {
            return traceLookupInternal(targetOffset);
        } catch (IOException e) {
            throw new UncheckedIOException("Lookup failed", e);
        }
    }

    /**
     * Locates an offset the way Kafka does: floor-lookup of the segment by base offset, binary search
     * of the sparse index for the greatest entry &lt;= target, then a linear scan of the .log file from
     * that physical position.
     */
    private LookupTrace traceLookupInternal(long target) throws IOException {
        if (target < logStartOffset || target >= logEndOffset) {
            return new LookupTrace(target, false, null, null, null, null, 0, 0, 0, 0, null,
                    "Offset out of range: valid range is [" + logStartOffset + ", " + logEndOffset + ")");
        }
        LogSegment segment = segments.floorEntry(target).getValue();
        int relative = (int) (target - segment.baseOffset);

        int lo = 0;
        int hi = segment.index.size() - 1;
        int best = -1;
        int steps = 0;
        while (lo <= hi) {
            steps++;
            int mid = (lo + hi) >>> 1;
            if (segment.index.get(mid).relativeOffset() <= relative) {
                best = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        IndexEntry used = best >= 0 ? segment.index.get(best) : null;
        long startPosition = used == null ? 0 : used.position();

        long position = startPosition;
        int scannedRecords = 0;
        int scannedBytes = 0;
        Integer foundPosition = null;
        while (position < segment.sizeBytes) {
            RecordView rec = segment.readAt(position);
            scannedRecords++;
            scannedBytes += rec.sizeBytes();
            if (rec.offset() == target) {
                foundPosition = (int) position;
                break;
            }
            position += rec.sizeBytes();
        }

        IndexEntryView usedView = used == null ? null
                : new IndexEntryView(used.relativeOffset(), segment.baseOffset + used.relativeOffset(),
                used.position());
        String explanation = "1) Segment " + segment.fileBaseName() + ".log chosen (greatest base offset <= "
                + target + "). 2) Binary search of " + segment.index.size() + " sparse index entries took "
                + steps + " step(s) and "
                + (used == null ? "found no entry <= relative offset " + relative + ", so the scan starts at byte 0."
                : "selected entry (relative " + used.relativeOffset() + " -> byte " + used.position() + ").")
                + " 3) Scanned " + scannedRecords + " record(s) / " + scannedBytes + " bytes to reach offset "
                + target + ".";

        return new LookupTrace(target, foundPosition != null, segment.baseOffset,
                segment.fileBaseName() + ".log", relative, usedView, steps, startPosition,
                scannedRecords, scannedBytes, foundPosition, explanation);
    }

    // ------------------------------------------------------------------
    // Inspection
    // ------------------------------------------------------------------

    public synchronized StorageSegmentView inspect(Long lookupOffset, int maxRecordsPerSegment) {
        try {
            int maxRecords = Math.max(1, Math.min(maxRecordsPerSegment, 500));
            List<SegmentInfo> infos = new ArrayList<>();
            for (LogSegment segment : segments.values()) {
                infos.add(describe(segment, maxRecords));
            }

            List<ReplicaView> replicas = new ArrayList<>();
            replicas.add(new ReplicaView("leader", "LEADER", logEndOffset, 0, true, false));
            for (String id : FOLLOWER_IDS) {
                long leo = followerLogEndOffsets.get(id);
                replicas.add(new ReplicaView(id, "FOLLOWER", leo, logEndOffset - leo, isInSync(id),
                        pausedFollowers.contains(id)));
            }

            List<String> notes = List.of(
                    "LEO (" + logEndOffset + ") is the next offset the leader will assign.",
                    "HW (" + highWatermark + ") = min(LEO of ISR); consumers only read offsets < HW.",
                    (logEndOffset - highWatermark) + " record(s) are written but not yet committed (LEO - HW).",
                    "The sparse index adds an entry roughly every " + indexIntervalBytes
                            + " bytes; lookups binary-search the index, then scan the .log forward.",
                    "Segments roll when the active .log would exceed " + segmentBytes + " bytes.");

            return StorageSegmentView.builder()
                    .topic(topic)
                    .partition(partition)
                    .partitionDirectory(partitionDir.toString())
                    .generatedAt(Instant.now().toString())
                    .config(new ConfigView(segmentBytes, indexIntervalBytes, maxFollowerLag))
                    .logStartOffset(logStartOffset)
                    .logEndOffset(logEndOffset)
                    .highWatermark(highWatermark)
                    .messagesAboveHighWatermark(logEndOffset - highWatermark)
                    .replicas(replicas)
                    .inSyncReplicas(inSyncReplicaIds())
                    .activeSegment(infos.get(infos.size() - 1))
                    .segments(infos)
                    .lookupTrace(lookupOffset == null ? null : traceLookupInternal(lookupOffset))
                    .notes(notes)
                    .build();
        } catch (IOException e) {
            throw new UncheckedIOException("Inspect failed", e);
        }
    }

    private SegmentInfo describe(LogSegment segment, int maxRecords) throws IOException {
        List<IndexEntryView> indexViews = segment.index.stream()
                .map(e -> new IndexEntryView(e.relativeOffset(), segment.baseOffset + e.relativeOffset(),
                        e.position()))
                .toList();
        List<TimeIndexEntryView> timeViews = segment.timeIndex.stream()
                .map(e -> new TimeIndexEntryView(e.timestamp(), Instant.ofEpochMilli(e.timestamp()).toString(),
                        e.relativeOffset(), segment.baseOffset + e.relativeOffset()))
                .toList();
        long hw = highWatermark;
        List<RecordView> records = segment.scan(maxRecords).stream()
                .map(r -> r.markVisibility(hw).abbreviated(80))
                .toList();
        return new SegmentInfo(
                segment.fileBaseName() + ".log",
                segment.fileBaseName() + ".index",
                segment.fileBaseName() + ".timeindex",
                segment.baseOffset,
                segment.nextOffset,
                segment == activeSegment,
                segment.recordCount,
                segment.sizeBytes,
                (long) segment.index.size() * OFFSET_INDEX_ENTRY_BYTES,
                (long) segment.timeIndex.size() * TIME_INDEX_ENTRY_BYTES,
                segment.maxTimestamp,
                indexViews,
                timeViews,
                records,
                segment.recordCount > records.size());
    }

    // ------------------------------------------------------------------
    // Internal types
    // ------------------------------------------------------------------

    private record IndexEntry(int relativeOffset, int position) {
    }

    private record TimeIndexEntry(long timestamp, int relativeOffset) {
    }

    private record SegmentAppend(long offset, int position, int totalBytes, long timestamp, boolean indexed) {
    }

    /** One .log/.index/.timeindex triple. */
    private static final class LogSegment implements Closeable {

        final long baseOffset;
        final Path logPath;
        final Path indexPath;
        final Path timeIndexPath;
        final FileChannel logChannel;
        final FileChannel indexChannel;
        final FileChannel timeIndexChannel;
        final List<IndexEntry> index = new ArrayList<>();
        final List<TimeIndexEntry> timeIndex = new ArrayList<>();

        long sizeBytes = 0;
        long nextOffset;
        int recordCount = 0;
        long maxTimestamp = -1;
        int bytesSinceLastIndexEntry = 0;

        LogSegment(Path dir, long baseOffset) throws IOException {
            this.baseOffset = baseOffset;
            this.nextOffset = baseOffset;
            String name = fileBaseName();
            this.logPath = dir.resolve(name + ".log");
            this.indexPath = dir.resolve(name + ".index");
            this.timeIndexPath = dir.resolve(name + ".timeindex");
            StandardOpenOption[] opts = {StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE};
            this.logChannel = FileChannel.open(logPath, opts);
            this.indexChannel = FileChannel.open(indexPath, opts);
            this.timeIndexChannel = FileChannel.open(timeIndexPath, opts);
        }

        String fileBaseName() {
            return String.format("%020d", baseOffset);
        }

        static int encodedSize(String key, String value) {
            int keyLen = key == null ? 0 : key.getBytes(StandardCharsets.UTF_8).length;
            int valueLen = value == null ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
            // header(12) + crc(4) + timestamp(8) + keyLen(4) + key + valueLen(4) + value
            return RECORD_HEADER_BYTES + 4 + 8 + 4 + keyLen + 4 + valueLen;
        }

        SegmentAppend append(String key, String value, long timestamp, int indexIntervalBytes)
                throws IOException {
            byte[] keyBytes = key == null ? null : key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
            int size = 4 + 8 + 4 + (keyBytes == null ? 0 : keyBytes.length)
                    + 4 + (valueBytes == null ? 0 : valueBytes.length);

            long offset = nextOffset;
            ByteBuffer buf = ByteBuffer.allocate(RECORD_HEADER_BYTES + size);
            buf.putLong(offset);
            buf.putInt(size);
            int crcPosition = buf.position();
            buf.putInt(0); // crc placeholder
            int payloadStart = buf.position();
            buf.putLong(timestamp);
            if (keyBytes == null) {
                buf.putInt(-1);
            } else {
                buf.putInt(keyBytes.length);
                buf.put(keyBytes);
            }
            if (valueBytes == null) {
                buf.putInt(-1);
            } else {
                buf.putInt(valueBytes.length);
                buf.put(valueBytes);
            }
            CRC32 crc = new CRC32();
            crc.update(buf.array(), payloadStart, buf.position() - payloadStart);
            buf.putInt(crcPosition, (int) crc.getValue());
            buf.flip();

            int position = (int) sizeBytes;
            int totalBytes = buf.remaining();
            long writePos = sizeBytes;
            while (buf.hasRemaining()) {
                writePos += logChannel.write(buf, writePos);
            }
            sizeBytes += totalBytes;
            nextOffset = offset + 1;
            recordCount++;
            bytesSinceLastIndexEntry += totalBytes;

            boolean indexed = false;
            if (bytesSinceLastIndexEntry >= indexIntervalBytes) {
                addIndexEntry((int) (offset - baseOffset), position);
                if (timestamp > maxTimestamp) {
                    addTimeIndexEntry(timestamp, (int) (offset - baseOffset));
                }
                bytesSinceLastIndexEntry = 0;
                indexed = true;
            }
            maxTimestamp = Math.max(maxTimestamp, timestamp);
            return new SegmentAppend(offset, position, totalBytes, timestamp, indexed);
        }

        private void addIndexEntry(int relativeOffset, int position) throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(OFFSET_INDEX_ENTRY_BYTES);
            buf.putInt(relativeOffset).putInt(position).flip();
            long pos = (long) index.size() * OFFSET_INDEX_ENTRY_BYTES;
            while (buf.hasRemaining()) {
                pos += indexChannel.write(buf, pos);
            }
            index.add(new IndexEntry(relativeOffset, position));
        }

        private void addTimeIndexEntry(long timestamp, int relativeOffset) throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(TIME_INDEX_ENTRY_BYTES);
            buf.putLong(timestamp).putInt(relativeOffset).flip();
            long pos = (long) timeIndex.size() * TIME_INDEX_ENTRY_BYTES;
            while (buf.hasRemaining()) {
                pos += timeIndexChannel.write(buf, pos);
            }
            timeIndex.add(new TimeIndexEntry(timestamp, relativeOffset));
        }

        RecordView readAt(long position) throws IOException {
            ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_BYTES);
            readFully(position, header);
            header.flip();
            long offset = header.getLong();
            int size = header.getInt();

            ByteBuffer body = ByteBuffer.allocate(size);
            readFully(position + RECORD_HEADER_BYTES, body);
            body.flip();

            int storedCrc = body.getInt();
            CRC32 crc = new CRC32();
            crc.update(body.array(), 4, size - 4);

            long timestamp = body.getLong();
            int keyLen = body.getInt();
            String key = null;
            if (keyLen >= 0) {
                byte[] kb = new byte[keyLen];
                body.get(kb);
                key = new String(kb, StandardCharsets.UTF_8);
            }
            int valueLen = body.getInt();
            String value = null;
            if (valueLen >= 0) {
                byte[] vb = new byte[valueLen];
                body.get(vb);
                value = new String(vb, StandardCharsets.UTF_8);
            }
            return new RecordView(offset, (int) position, RECORD_HEADER_BYTES + size, timestamp, key, value,
                    storedCrc == (int) crc.getValue(), false);
        }

        List<RecordView> scan(int maxRecords) throws IOException {
            List<RecordView> records = new ArrayList<>();
            long position = 0;
            while (position < sizeBytes && records.size() < maxRecords) {
                RecordView rec = readAt(position);
                records.add(rec);
                position += rec.sizeBytes();
            }
            return records;
        }

        private void readFully(long position, ByteBuffer buf) throws IOException {
            long pos = position;
            while (buf.hasRemaining()) {
                int n = logChannel.read(buf, pos);
                if (n < 0) {
                    throw new EOFException("Unexpected end of " + logPath + " at position " + pos);
                }
                pos += n;
            }
        }

        void seal() throws IOException {
            logChannel.force(false);
            indexChannel.force(false);
            timeIndexChannel.force(false);
        }

        @Override
        public void close() throws IOException {
            try {
                seal();
            } finally {
                logChannel.close();
                indexChannel.close();
                timeIndexChannel.close();
            }
        }
    }
}
