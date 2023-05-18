package com.adaptris.interlok.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.jdbc.JdbcConnection;
import com.adaptris.core.util.JdbcUtil;
import com.adaptris.interlok.junit.scaffolding.services.JdbcServiceCase;

public abstract class JDBCStatementBuilderCase extends JdbcServiceCase {
  protected static final String JDBC_QUERYSERVICE_DRIVER = "jdbc.queryservice.driver";
  protected static final String JDBC_QUERYSERVICE_URL = "jdbc.queryservice.url";

  private static final String NAME = "Rachel";
  private static final String DOB = "1990-03-19";
  private static final String AGE = Integer
      .toString(Period.between(LocalDate.parse(DOB, DateTimeFormatter.ISO_LOCAL_DATE), LocalDate.now()).getYears());

  @Test
  public void testServicesCaptureQuery() throws Exception {
    createDatabase();

    JdbcConnection connection = new JdbcConnection(PROPERTIES.getProperty(JDBC_QUERYSERVICE_URL),
        PROPERTIES.getProperty(JDBC_QUERYSERVICE_DRIVER));

    JDBCStatementBuilder service = JDBCCaptureStatementBuilderTest.getService();
    service.setConnection(connection);
    service.initService();

    AdaptrisMessage message = AdaptrisMessageFactory.getDefaultInstance().newMessage(NAME);
    message.addMetadata("dob", DOB);
    message.addMetadata("age", AGE);

    execute(service, message);

    service = JDBCQueryStatementBuilderTest.getService();
    service.setConnection(connection);
    service.initService();

    message = AdaptrisMessageFactory.getDefaultInstance().newMessage(message.getUniqueId());

    execute(service, message);

    log.debug(message.getContent());
  }

  protected static void createDatabase() throws Exception {
    Connection c = null;
    Statement s = null;
    try {
      Class.forName(PROPERTIES.getProperty(JDBC_QUERYSERVICE_DRIVER));
      c = DriverManager.getConnection(PROPERTIES.getProperty(JDBC_QUERYSERVICE_URL));
      c.setAutoCommit(true);
      s = c.createStatement();
      try {
        s.execute("DROP TABLE person");
      } catch (Exception e) {
        // Ignore exceptions from the drop
      }
      s.execute("CREATE TABLE person "
          + "(id VARCHAR(128) NOT NULL, name VARCHAR(128) NOT NULL, dob DATE NOT NULL, age INT, inserted_on TIMESTAMP)");
    } finally {
      JdbcUtil.closeQuietly(s);
      JdbcUtil.closeQuietly(c);
    }
  }

}
