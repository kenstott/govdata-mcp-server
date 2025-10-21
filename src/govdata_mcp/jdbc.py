"""JDBC connection manager using JPype to connect to Calcite."""

import jpype
import jpype.dbapi2 as dbapi2
from typing import Optional, Any, List, Tuple
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class CalciteConnection:
    """Manages JDBC connection to Apache Calcite using JPype."""

    def __init__(self, jar_path: str, model_path: str):
        """
        Initialize Calcite JDBC connection.

        Args:
            jar_path: Path to Calcite fat JAR file
            model_path: Path to Calcite model JSON file
        """
        self.jar_path = jar_path
        self.model_path = model_path
        self._connection: Optional[Any] = None
        self._initialize_jvm()

    def _initialize_jvm(self) -> None:
        """Start JVM if not already started and load Calcite JAR."""
        if not jpype.isJVMStarted():
            logger.info(f"Starting JVM with Calcite JAR: {self.jar_path}")

            # Find project root and additional JARs
            project_root = Path(__file__).parent.parent.parent
            log4j_path = project_root / "log4j.properties"
            slf4j_binding_jar = project_root / "lib" / "slf4j-reload4j-2.0.13.jar"
            duckdb_jdbc_jar = project_root / "lib" / "duckdb-jdbc-1.1.3.jar"

            # Build classpath - add SLF4J 2.x binding and DuckDB JDBC driver
            classpath_jars = []

            if slf4j_binding_jar.exists():
                classpath_jars.append(str(slf4j_binding_jar))
                logger.info(f"Added SLF4J 2.x binding JAR to classpath")
            else:
                logger.warning(f"SLF4J binding JAR not found at {slf4j_binding_jar}")

            if duckdb_jdbc_jar.exists():
                classpath_jars.append(str(duckdb_jdbc_jar))
                logger.info(f"Added DuckDB JDBC driver to classpath")
            else:
                logger.warning(f"DuckDB JDBC JAR not found at {duckdb_jdbc_jar} - DuckDB execution may fail")

            # Add Calcite JAR last
            classpath_jars.append(self.jar_path)
            classpath = os.pathsep.join(classpath_jars)

            # JVM arguments for memory and logging configuration
            jvm_args = [
                "-Xmx8g",  # Maximum heap size
                "-Xms2g",  # Initial heap size
                "-XX:+UseG1GC",  # Use G1 garbage collector
                "-XX:MaxGCPauseMillis=200",  # Max GC pause time
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=error",  # Suppress SLF4J warnings
                "-Dorg.apache.calcite.adapter.govdata.level=DEBUG",  # Enable govdata adapter logging
            ]

            # Add log4j config if file exists
            if log4j_path.exists():
                # Use file: URL (single slash for local files, reload4j/log4j 1.x format)
                log4j_url = f"file:{log4j_path.absolute()}"
                jvm_args.append(f"-Dlog4j.configuration={log4j_url}")
                logger.info(f"Using log4j config: {log4j_url}")
            else:
                logger.warning(f"log4j.properties not found at {log4j_path}")

            jpype.startJVM(classpath=classpath, *jvm_args, convertStrings=False)

            # Always configure log4j programmatically to ensure it works
            # File-based config via system property is unreliable with JPype
            self._configure_log4j_programmatically()

            # Force SLF4J to initialize with the reload4j binding
            self._initialize_slf4j()

            logger.info("JVM started successfully - log4j and SLF4J configured")
        else:
            logger.info("JVM already running")

    def _configure_log4j_programmatically(self) -> None:
        """Configure log4j/reload4j programmatically via JPype."""
        try:
            # Import log4j classes (works with reload4j as well)
            Logger = jpype.JClass("org.apache.log4j.Logger")
            ConsoleAppender = jpype.JClass("org.apache.log4j.ConsoleAppender")
            PatternLayout = jpype.JClass("org.apache.log4j.PatternLayout")
            Level = jpype.JClass("org.apache.log4j.Level")

            # Configure root logger
            root_logger = Logger.getRootLogger()

            # Remove any existing appenders first
            root_logger.removeAllAppenders()

            root_logger.setLevel(Level.INFO)

            # Create console appender with pattern
            pattern = "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"
            console_appender = ConsoleAppender(PatternLayout(pattern))
            root_logger.addAppender(console_appender)

            # Reduce AWS SDK verbosity
            Logger.getLogger("com.amazonaws").setLevel(Level.WARN)
            Logger.getLogger("org.apache.http").setLevel(Level.WARN)

            # Calcite logging
            Logger.getLogger("org.apache.calcite").setLevel(Level.INFO)
            Logger.getLogger("org.apache.calcite.plan").setLevel(Level.WARN)
            Logger.getLogger("org.apache.calcite.rel").setLevel(Level.WARN)
            Logger.getLogger("org.apache.calcite.sql2rel").setLevel(Level.WARN)

            # Govdata adapter - set to DEBUG for detailed logging
            govdata_logger = Logger.getLogger("org.apache.calcite.adapter.govdata")
            govdata_logger.setLevel(Level.DEBUG)

            logger.info("log4j configured programmatically")

            # Test that Java logging is working by emitting a test log statement
            test_logger = Logger.getLogger("govdata_mcp.test")
            test_logger.info("Java log4j configuration test - if you see this, logging is working!")

        except Exception as e:
            logger.error(f"Failed to configure log4j programmatically: {e}", exc_info=True)

    def _initialize_slf4j(self) -> None:
        """Verify SLF4J binding to reload4j."""
        try:
            # Get SLF4J LoggerFactory and verify the binding
            LoggerFactory = jpype.JClass("org.slf4j.LoggerFactory")

            # Get a logger and check what implementation is being used
            test_logger = LoggerFactory.getLogger("org.apache.calcite.adapter.govdata")
            logger_class = test_logger.getClass().getName()
            logger.info(f"SLF4J logger implementation: {logger_class}")

            # If using NOP, warn that logging won't work
            if "NOP" in logger_class:
                logger.warning("SLF4J still using NOP logger - Calcite logs will not appear")
                logger.warning("Make sure lib/slf4j-reload4j-2.0.13.jar exists and is in the classpath")
            else:
                # Emit a test message via SLF4J to verify it works
                test_logger.info("SLF4J binding successful - Calcite logging is enabled!")

        except Exception as e:
            logger.error(f"Failed to verify SLF4J: {e}", exc_info=True)

    def connect(self) -> None:
        """Establish connection to Calcite."""
        if self._connection is None:
            jdbc_url = f"jdbc:calcite:model={self.model_path}"
            logger.info(f"Connecting to Calcite: {jdbc_url}")
            logger.info("Note: Watch for Calcite/govdata adapter logs below...")
            self._connection = dbapi2.connect(
                jdbc_url, driver="org.apache.calcite.jdbc.Driver"
            )
            logger.info("Connected to Calcite successfully")
            logger.info("Connection established - checking if any Calcite logs appeared above")

    def get_cursor(self):
        """Get a database cursor for executing queries."""
        if self._connection is None:
            self.connect()
        return self._connection.cursor()

    def execute_query(self, sql: str) -> Tuple[List[str], List[Tuple]]:
        """
        Execute SQL query and return column names and rows.

        Args:
            sql: SQL query to execute

        Returns:
            Tuple of (column_names, rows)
        """
        cursor = self.get_cursor()
        try:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return columns, rows
        finally:
            cursor.close()

    def execute_metadata_query(self, sql: str) -> List[dict]:
        """
        Execute metadata query and return results as list of dicts.

        Args:
            sql: SQL query to execute

        Returns:
            List of dictionaries with column names as keys
        """
        columns, rows = self.execute_query(sql)
        return [dict(zip(columns, row)) for row in rows]

    def close(self) -> None:
        """Close the connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Calcite connection closed")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Global connection instance (initialized on server startup)
_connection: Optional[CalciteConnection] = None


def get_connection() -> CalciteConnection:
    """Get the global Calcite connection instance."""
    if _connection is None:
        raise RuntimeError("Calcite connection not initialized. Call initialize_connection() first.")
    return _connection


def initialize_connection(jar_path: str, model_path: str) -> None:
    """Initialize the global Calcite connection."""
    global _connection
    _connection = CalciteConnection(jar_path, model_path)
    _connection.connect()
    logger.info("Global Calcite connection initialized")