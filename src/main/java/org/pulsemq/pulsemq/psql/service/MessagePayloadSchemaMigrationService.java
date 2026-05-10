package org.pulsemq.pulsemq.psql.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.pulsemq.pulsemq.common.enums.MessageStatus;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessagePayloadSchemaMigrationService {

    private final JdbcTemplate jdbcTemplate;

    @EventListener(ApplicationReadyEvent.class)
    @Transactional
    public void migrateMessagePayloadColumnIfNeeded() {
        ensureMessageStatusCheckConstraintSupportsPurged();

        if (!isOidPayloadColumn()) {
            log.info("messages.payload column already uses text storage; no migration needed");
            return;
        }

        log.info("Migrating messages.payload from oid to text");
        jdbcTemplate.execute("""
                ALTER TABLE messages
                ALTER COLUMN payload TYPE TEXT
                USING convert_from(lo_get(payload), 'UTF8')
                """);
        log.info("Completed messages.payload migration to text");
    }

    private void ensureMessageStatusCheckConstraintSupportsPurged() {
        log.info("Ensuring messages.status check constraint supports PURGED");
        jdbcTemplate.execute("""
                ALTER TABLE messages
                DROP CONSTRAINT IF EXISTS messages_status_check
                """);

        jdbcTemplate.execute("""
                ALTER TABLE messages
                ADD CONSTRAINT messages_status_check
                CHECK (status IN (%s))
                """.formatted(String.join(
                ", ",
                java.util.Arrays.stream(MessageStatus.values())
                        .map(status -> "'" + status.name() + "'")
                        .toList()
        )));
        log.info("messages.status check constraint updated successfully");
    }

    private boolean isOidPayloadColumn() {
        String dataType = jdbcTemplate.queryForObject("""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_name = 'messages'
                  AND column_name = 'payload'
                """, String.class);
        return dataType != null && dataType.equalsIgnoreCase("oid");
    }
}

