package org.pulsemq.pulsemq.service.wal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Service
@RequiredArgsConstructor
@Slf4j
public class WalWriterService {

    private static final DateTimeFormatter ROTATION_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").withZone(ZoneOffset.UTC);

    private final ObjectMapper objectMapper;
    private final WalProperties walProperties;

    private BufferedWriter writer;
    private Path currentFilePath;
    private long currentFileSize;
    private int rotationSequence;

    public synchronized void appendEvent(WalEvent event) {
        if (!walProperties.isEnabled() || event == null) {
            return;
        }

        try {
            String serialized = objectMapper.writeValueAsString(event);
            byte[] payloadBytes = (serialized + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
            ensureWriterOpen();
            rotateIfNeeded(payloadBytes.length);
            writer.write(serialized);
            writer.newLine();
            writer.flush();
            currentFileSize += payloadBytes.length;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize WAL event", e);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append WAL event", e);
        }
    }

    public synchronized void flush() {
        if (writer == null) {
            return;
        }

        try {
            writer.flush();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to flush WAL writer", e);
        }
    }

    @PreDestroy
    public synchronized void close() {
        if (writer == null) {
            return;
        }

        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            log.warn("Failed to close WAL writer cleanly", e);
        } finally {
            writer = null;
        }
    }

    private void ensureWriterOpen() throws IOException {
        if (writer != null) {
            return;
        }

        Path directory = Path.of(walProperties.getDirectory());
        Files.createDirectories(directory);
        Files.createDirectories(Path.of(walProperties.getArchiveDirectory()));

        currentFilePath = directory.resolve(walProperties.getFileName());
        currentFileSize = Files.exists(currentFilePath) ? Files.size(currentFilePath) : 0L;
        writer = Files.newBufferedWriter(
                currentFilePath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE
        );
    }

    private void rotateIfNeeded(long nextWriteSize) throws IOException {
        if (currentFilePath == null) {
            return;
        }

        long maxSize = walProperties.getMaxFileSizeBytes();
        if (currentFileSize + nextWriteSize <= maxSize) {
            return;
        }

        writer.flush();
        writer.close();

        Path archiveDirectory = Path.of(walProperties.getArchiveDirectory());
        Files.createDirectories(archiveDirectory);

        String archiveName = currentFilePath.getFileName() + "." + ROTATION_FORMAT.format(Instant.now()) + "-" + String.format("%04d", rotationSequence++) + ".log";
        Path archivedPath = archiveDirectory.resolve(archiveName);
        if (Files.exists(currentFilePath)) {
            Files.move(currentFilePath, archivedPath, StandardCopyOption.REPLACE_EXISTING);
        }

        writer = Files.newBufferedWriter(
                currentFilePath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE
        );
        currentFileSize = 0L;
    }
}


