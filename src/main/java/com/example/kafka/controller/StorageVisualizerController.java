package com.example.kafka.controller;

import com.example.kafka.model.StorageSegmentView;
import com.example.kafka.simulator.KafkaStorageEngineSimulator;
import com.example.kafka.simulator.KafkaStorageEngineSimulator.AppendResult;
import com.example.kafka.simulator.KafkaStorageEngineSimulator.ReadResult;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST facade over {@link KafkaStorageEngineSimulator}.
 */
@RestController
@RequestMapping("/api/v1/log-engine")
@RequiredArgsConstructor
public class StorageVisualizerController {

    private static final int MAX_SEED = 10_000;

    private final KafkaStorageEngineSimulator engine;

    public record AppendRequest(String key, @NotBlank String value, Boolean replicate) {
    }

    /** Active segment state, byte positions, sparse index entries; optional lookup trace. */
    @GetMapping("/inspect")
    public StorageSegmentView inspect(
            @RequestParam(required = false) Long lookupOffset,
            @RequestParam(defaultValue = "50") int maxRecordsPerSegment) {
        return engine.inspect(lookupOffset, maxRecordsPerSegment);
    }

    @PostMapping("/append")
    public AppendResult append(@Valid @RequestBody AppendRequest request) {
        boolean replicate = request.replicate() == null || request.replicate();
        return engine.append(request.key(), request.value(), replicate);
    }

    /** Generates sample records; with replicate=false the HW lags behind the LEO. */
    @PostMapping("/seed")
    public Map<String, Object> seed(@RequestParam(defaultValue = "20") int count,
                                    @RequestParam(defaultValue = "true") boolean replicate) {
        int n = Math.max(1, Math.min(count, MAX_SEED));
        AppendResult last = null;
        int rolled = 0;
        for (int i = 0; i < n; i++) {
            String key = "customer-" + (i % 5);
            String value = String.format("{\"orderId\":\"SEED-%d\",\"product\":\"widget-%d\",\"amount\":%d}",
                    i, i % 7, 10 + (i * 13) % 500);
            last = engine.append(key, value, replicate);
            if (last.segmentRolled()) {
                rolled++;
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("recordsAppended", n);
        result.put("segmentsRolledDuringSeed", rolled);
        result.put("lastAppend", last);
        return result;
    }

    @PostMapping("/replicate")
    public Map<String, Object> replicate(@RequestParam String follower,
                                         @RequestParam(defaultValue = "1") long count) {
        return engine.replicate(follower, count);
    }

    @PostMapping("/followers/{id}/pause")
    public Map<String, Object> pause(@PathVariable String id) {
        return engine.pauseFollower(id);
    }

    @PostMapping("/followers/{id}/resume")
    public Map<String, Object> resume(@PathVariable String id) {
        return engine.resumeFollower(id);
    }

    @PostMapping("/roll")
    public Map<String, Object> roll() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("newActiveSegmentBaseOffset", engine.roll());
        return result;
    }

    /** Consumer-style read bounded by the high watermark. */
    @GetMapping("/read")
    public ReadResult read(@RequestParam long offset, @RequestParam(defaultValue = "10") int max) {
        return engine.read(offset, max);
    }

    @PostMapping("/reset")
    public Map<String, String> reset() {
        engine.reset();
        return Map.of("status", "RESET");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> conflict(IllegalStateException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
    }
}
