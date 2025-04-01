package org.example;

import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.container.jdisc.ThreadedHttpRequestHandler;
import com.yahoo.jdisc.Response;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.time.Instant;
import java.time.Duration;
import org.json.JSONObject;

public class CustomIndexingHandler extends ThreadedHttpRequestHandler {

    private static final Logger log = Logger.getLogger(CustomIndexingHandler.class.getName());

    // Global counters to track processed documents
    private static final AtomicInteger totalProcessedDocuments = new AtomicInteger(0);
    private static final ConcurrentHashMap<String, Integer> fileChunkCounts = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Integer> fileProcessedCounts = new ConcurrentHashMap<>();

    public CustomIndexingHandler(Executor executor) {
        super(executor);
    }

    @Override
    public HttpResponse handle(HttpRequest request) {
        // Only POST is supported
        if ("POST".equalsIgnoreCase(request.getMethod().toString())) {
            Instant startTime = Instant.now();

            try {
                log.info("== Starting processing of new indexing request ==");

                // Reading request data
                log.info("Reading request data...");
                BufferedReader reader = new BufferedReader(new InputStreamReader(request.getData(), StandardCharsets.UTF_8));
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                String payload = sb.toString();

                // Calculate request size in kilobytes
                double payloadSizeKB = payload.getBytes(StandardCharsets.UTF_8).length / 1024.0;
                log.info(String.format("Received indexing request of size %.2f KB", payloadSizeKB));

                // Parse JSON
                log.info("Parsing JSON...");
                JSONObject json = new JSONObject(payload);
                String content = json.getString("content");

                // Read additional metadata fields
                String fileIdentifier = json.optString("fileIdentifier", "unknown");
                String fileName = json.optString("fileName", "unknown");
                int sequenceNumber = json.optInt("sequenceNumber", 0);
                int totalChunks = json.optInt("totalChunks", 0);

                // Update counters
                fileChunkCounts.putIfAbsent(fileIdentifier, totalChunks);
                int currentProcessed = fileProcessedCounts.getOrDefault(fileIdentifier, 0);
                fileProcessedCounts.put(fileIdentifier, currentProcessed + 1);
                int docNumber = totalProcessedDocuments.incrementAndGet();

                // Log chunk information
                log.info(String.format("Processing chunk %d/%d from file '%s' (ID: %s)",
                        sequenceNumber, totalChunks, fileName, fileIdentifier));
                log.info(String.format("Content length: %d characters", content.length()));

                // Here you would implement integration with Vespa document indexing
                // Measure processing time
                Instant indexingStartTime = Instant.now();
                log.info("Starting document indexing...");

                // Simulate processing time (instead of actual indexing)
                // Thread.sleep(100); // Note: If you want to simulate processing, you can uncomment this line

                // Check if document was actually indexed
                boolean indexedSuccessfully = checkDocumentIndexed(content, fileIdentifier, sequenceNumber);
                Instant indexingEndTime = Instant.now();
                long indexingTimeMs = Duration.between(indexingStartTime, indexingEndTime).toMillis();

                log.info(String.format("Document indexing completed in %d milliseconds", indexingTimeMs));

                // Check if this is the last chunk from the file
                boolean isLastChunk = checkIfLastChunk(fileIdentifier);
                if (isLastChunk) {
                    log.info(String.format("Completed processing all chunks (%d) for file '%s'",
                            totalChunks, fileName));

                    // If this is the last chunk, verify that all documents from the file are available for search
                    log.info("Performing document availability check for search...");
                    boolean allAvailable = verifyAllDocumentsAvailable(fileIdentifier, totalChunks);
                    log.info("Availability check result: " + (allAvailable ? "All documents available" : "Some documents missing"));
                }

                // Prepare response
                Instant endTime = Instant.now();
                long totalProcessingTimeMs = Duration.between(startTime, endTime).toMillis();

                // Prepare response as JSON
                JSONObject responseBody = new JSONObject()
                        .put("indexing_success", true)
                        .put("document_number", docNumber)
                        .put("total_processed", totalProcessedDocuments.get())
                        .put("sequence_number", sequenceNumber)
                        .put("total_chunks", totalChunks)
                        .put("processing_time_ms", totalProcessingTimeMs)
                        .put("indexing_time_ms", indexingTimeMs)
                        .put("file_identifier", fileIdentifier)
                        .put("is_last_chunk", isLastChunk);

                log.info(String.format("== Request processing completed. Total time: %d milliseconds ==", totalProcessingTimeMs));

                return new CustomHttpResponse(200, responseBody.toString().getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                Instant errorTime = Instant.now();
                long processingTimeMs = Duration.between(startTime, errorTime).toMillis();

                log.severe(String.format("Error processing request (%d milliseconds): %s",
                        processingTimeMs, e.getMessage()));
                e.printStackTrace();

                JSONObject errorBody = new JSONObject()
                        .put("indexing_success", false)
                        .put("error", e.getMessage())
                        .put("processing_time_ms", processingTimeMs);

                return new CustomHttpResponse(500, errorBody.toString().getBytes(StandardCharsets.UTF_8));
            }
        }

        log.warning("Rejected non-POST method request");
        JSONObject errorBody = new JSONObject()
                .put("indexing_success", false)
                .put("error", "Method Not Allowed. Use POST.")
                .put("processing_time_ms", 0);

        return new CustomHttpResponse(405, errorBody.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Checks if a document was successfully indexed (replace with actual logic)
     */
    private boolean checkDocumentIndexed(String content, String fileId, int sequenceNumber) {
        // Implement actual logic to check if the document was indexed successfully
        log.info(String.format("Checking that document %s-%d was successfully indexed", fileId, sequenceNumber));
        // Dummy logic - always returns true
        return true;
    }

    /**
     * Checks if this is the last chunk from the current file
     */
    private boolean checkIfLastChunk(String fileId) {
        int totalChunks = fileChunkCounts.getOrDefault(fileId, 0);
        int processedChunks = fileProcessedCounts.getOrDefault(fileId, 0);
        boolean isLast = (totalChunks > 0 && processedChunks >= totalChunks);

        if (isLast) {
            log.info(String.format("Identified last chunk for file %s (%d/%d)",
                    fileId, processedChunks, totalChunks));
        }

        return isLast;
    }

    /**
     * Verifies that all documents from the file are available for search
     */
    private boolean verifyAllDocumentsAvailable(String fileId, int totalChunks) {
        // Implement actual logic to verify availability of all documents for search
        Instant startTime = Instant.now();

        // Simulate availability check (replace with actual code)
        // Thread.sleep(200); // Note: If you want to simulate verification, you can uncomment this line

        Instant endTime = Instant.now();
        long verificationTimeMs = Duration.between(startTime, endTime).toMillis();

        log.info(String.format("Availability check for %d documents from file %s completed in %d milliseconds",
                totalChunks, fileId, verificationTimeMs));

        return true; // Dummy logic - always returns true
    }
}