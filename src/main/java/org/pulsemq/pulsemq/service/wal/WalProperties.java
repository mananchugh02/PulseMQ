package org.pulsemq.pulsemq.service.wal;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "pulsemq.wal")
@Getter
@Setter
public class WalProperties {

    /**
     * Base directory for WAL files.
     */
    private String directory = "logs/wal";

    /**
     * Directory where rotated WAL files are archived.
     */
    private String archiveDirectory = "logs/wal/archive";

    /**
     * Current append-only WAL file name.
     */
    private String fileName = "pulsemq-wal.log";

    /**
     * Maximum size of a WAL file before rotation.
     */
    private long maxFileSizeBytes = 10L * 1024L * 1024L;

    private boolean enabled = true;
}


