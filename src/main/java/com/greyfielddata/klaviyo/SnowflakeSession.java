package com.greyfielddata.klaviyo;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Snowflake connection and session setup from environment variables.
 * Authentication is key-pair (JWT) only; no passwords are read.
 */
public final class SnowflakeSession {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Z][A-Z0-9_]{0,127}");
    private static final Pattern ACCOUNT = Pattern.compile("[A-Za-z0-9._-]{1,255}");

    public final String database;
    public final String schema;
    public final String warehouse;
    public final String role;

    private SnowflakeSession(String database, String schema, String warehouse, String role) {
        this.database = requireIdentifier(database);
        this.schema = requireIdentifier(schema);
        this.warehouse = requireIdentifier(warehouse);
        this.role = requireIdentifier(role);
    }

    /** Reads SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE. */
    public static SnowflakeSession fromEnv() {
        return new SnowflakeSession(
            env("SNOWFLAKE_DATABASE", "RAW").toUpperCase(),
            env("SNOWFLAKE_SCHEMA", "KLAVIYO").toUpperCase(),
            required("SNOWFLAKE_WAREHOUSE").toUpperCase(),
            env("SNOWFLAKE_ROLE", "KLAVIYO_PIPELINE_ROLE").toUpperCase()
        );
    }

    /** Fully qualified, validated table name in the configured database and schema. */
    public String table(String name) {
        return database + "." + schema + "." + requireIdentifier(name);
    }

    public Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), authentication());
    }

    /** Selects warehouse, role, database and schema, and bounds lock and statement time. */
    public void configure(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("USE WAREHOUSE " + warehouse);
            stmt.execute("USE ROLE " + role);
            stmt.execute("USE DATABASE " + database);
            stmt.execute("USE SCHEMA " + schema);
            stmt.execute(
                "ALTER SESSION SET LOCK_TIMEOUT=120, "
                + "STATEMENT_TIMEOUT_IN_SECONDS=200, ABORT_DETACHED_QUERY=TRUE"
            );
        }
    }

    static String jdbcUrl() {
        String explicit = System.getenv("SNOWFLAKE_JDBC_URL");
        if (explicit != null && explicit.length() > 0) {
            return explicit;
        }
        String account = required("SNOWFLAKE_ACCOUNT");
        if (!ACCOUNT.matcher(account).matches()) {
            throw new IllegalArgumentException("Invalid SNOWFLAKE_ACCOUNT");
        }
        return "jdbc:snowflake://" + account + ".snowflakecomputing.com/";
    }

    static Properties authentication() {
        Properties props = new Properties();
        props.setProperty("user", required("SNOWFLAKE_USER"));
        props.setProperty("authenticator", "snowflake_jwt");
        props.setProperty("private_key_file", required("SNOWFLAKE_PRIVATE_KEY_FILE"));
        String passphrase = System.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD");
        if (passphrase != null && passphrase.length() > 0) {
            props.setProperty("private_key_file_pwd", passphrase);
        }
        return props;
    }

    public static String requireIdentifier(String value) {
        if (value == null || !IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException("Invalid Snowflake identifier");
        }
        return value;
    }

    public static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.length() == 0 ? fallback : value;
    }

    public static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.length() == 0) {
            throw new IllegalStateException("Missing required environment variable " + name);
        }
        return value;
    }

    /** Reads a secret from a file named by an environment variable. */
    public static String readSecretFile(String envName) throws Exception {
        Path path = Path.of(required(envName));
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        String value = String.join("", lines).trim();
        if (value.length() == 0) {
            throw new IllegalStateException(envName + " file is empty");
        }
        return value;
    }

    /**
     * Rolls back and closes the connection on JVM shutdown (for example SIGTERM
     * from a timeout wrapper), so an interrupted worker never leaves an open
     * transaction holding locks on the state table.
     */
    public static final class ShutdownGuard implements AutoCloseable {
        private final Connection connection;
        private final Thread shutdownHook;
        private boolean cleaned;

        public ShutdownGuard(Connection connection, String threadName) {
            this.connection = connection;
            this.shutdownHook = new Thread(() -> {
                SQLException failure = cleanup(true);
                if (failure != null) {
                    System.err.println(
                        "Snowflake shutdown cleanup failed: " + failure.getMessage()
                    );
                }
            }, threadName);
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }

        @Override
        public void close() throws SQLException {
            SQLException failure = cleanup(false);
            if (isCleaned()) {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // JVM is already shutting down.
                }
            }
            if (failure != null) {
                throw failure;
            }
        }

        private synchronized SQLException cleanup(boolean rollback) {
            if (cleaned) {
                return null;
            }
            SQLException failure = null;
            if (rollback) {
                try {
                    if (!connection.getAutoCommit()) {
                        connection.rollback();
                    }
                } catch (SQLException e) {
                    failure = e;
                }
            }
            try {
                connection.close();
                cleaned = true;
            } catch (SQLException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
            return failure;
        }

        private synchronized boolean isCleaned() {
            return cleaned;
        }
    }
}
