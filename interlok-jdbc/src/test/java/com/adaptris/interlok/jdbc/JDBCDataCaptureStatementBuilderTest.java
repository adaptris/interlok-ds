package com.adaptris.interlok.jdbc;

import com.adaptris.core.services.jdbc.DateStatementParameter;
import com.adaptris.core.services.jdbc.IntegerStatementParameter;
import com.adaptris.core.services.jdbc.JdbcDataCaptureService;
import com.adaptris.core.services.jdbc.StatementParameterList;
import com.adaptris.core.services.jdbc.StringStatementParameter;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class JDBCDataCaptureStatementBuilderTest extends JDBCStatementBuilderCase
{
  @Test
  public void testCreateService() throws Exception
  {
    JDBCDataCaptureStatementBuilderService service = getService();
    service.initService();

    JdbcDataCaptureService queryService = getQueryService(service);
    StatementParameterList parameters = queryService.getStatementParameters();
    assertEquals(StringStatementParameter.class, parameters.getParameterByName("id").getClass());
    assertEquals(StringStatementParameter.class, parameters.getParameterByName("name").getClass());
    assertEquals(DateStatementParameter.class, parameters.getParameterByName("dob").getClass());
    assertEquals(IntegerStatementParameter.class, parameters.getParameterByName("age").getClass());
  }

  private static JdbcDataCaptureService getQueryService(JDBCDataCaptureStatementBuilderService service) throws Exception
  {
    Class c = JDBCDataCaptureStatementBuilderService.class;
    Field f = c.getDeclaredField("service");
    f.setAccessible(true);
    return (JdbcDataCaptureService)f.get(service);
  }

  protected static JDBCDataCaptureStatementBuilderService getService()
  {
    JDBCDataCaptureStatementBuilderService service = new JDBCDataCaptureStatementBuilderService();
    service.setStatement("INSERT INTO person (id, name, dob, age) VALUES (" +
        "%sql_id{string:id}, " +
        "%sql_payload{string:name}, " +
        "%sql_metadata{date:dob}, " +
        "%sql_metadata{integer:age})");
    return service;
  }

  @Override
  protected Object retrieveObjectForSampleConfig()
  {
    return getService();
  }
}
