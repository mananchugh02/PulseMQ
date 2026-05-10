package org.pulsemq.pulsemq.psql.model;

import jakarta.persistence.*;
import lombok.*;
import org.pulsemq.pulsemq.common.enums.MessageStatus;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "messages",
        indexes = {
                @Index(name = "idx_queue_status", columnList = "queue_id,status"),
                @Index(name = "idx_visible_at", columnList = "visible_at")
        }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "queue_id", nullable = false)
    private QueueEntity queue;

    @Lob
    @Column(nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private MessageStatus status;

    @Column(nullable = false)
    private Integer retryCount;

    @Column(name = "visible_at", nullable = false)
    private Instant visibleAt;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    @PrePersist
    public void prePersist() {
        Instant now = Instant.now();

        createdAt = now;
        updatedAt = now;

        if (retryCount == null) {
            retryCount = 0;
        }

        if (visibleAt == null) {
            visibleAt = now;
        }

        if (status == null) {
            status = MessageStatus.READY;
        }
    }

    @PreUpdate
    public void preUpdate() {
        updatedAt = Instant.now();
    }
}