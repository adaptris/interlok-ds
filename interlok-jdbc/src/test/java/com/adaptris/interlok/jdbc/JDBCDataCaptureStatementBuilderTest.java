package com.adaptris.interlok.jdbc;

import com.adaptris.core.services.jdbc.BooleanStatementParameter;
import com.adaptris.core.services.jdbc.DateStatementParameter;
import com.adaptris.core.services.jdbc.DoubleStatementParameter;
import com.adaptris.core.services.jdbc.FloatStatementParameter;
import com.adaptris.core.services.jdbc.IntegerStatementParameter;
import com.adaptris.core.services.jdbc.JdbcDataCaptureService;
import com.adaptris.core.services.jdbc.LongStatementParameter;
import com.adaptris.core.services.jdbc.ShortStatementParameter;
import com.adaptris.core.services.jdbc.StatementParameterList;
import com.adaptris.core.services.jdbc.StringStatementParameter;
import com.adaptris.core.services.jdbc.TimeStatementParameter;
import com.adaptris.core.services.jdbc.TimestampStatementParameter;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class JDBCDataCaptureStatementBuilderTest extends JDBCStatementBuilderCase
{
  @Test
  public void testCreateService() throws Exception
  {
    JDBCDataCaptureStatementBuilderService service = new JDBCDataCaptureStatementBuilderService();
    service.setStatement("INSERT INTO everything (b, s, i, l, f, d, r, t, a, m) VALUES (" +
            "%sql_metadata{boolean:b}, " +
            "%sql_metadata{short:s}, " +
            "%sql_metadata{integer:i}, " +
            "%sql_metadata{long:l}, " +
            "%sql_metadata{float:f}, " +
            "%sql_metadata{double:d}, " +
            "%sql_metadata{string:r}, " +
            "%sql_metadata{time:t}, " +
            "%sql_metadata{date:a}, " +
            "%sql_metadata{timestamp:m})");
    service.initService();

    JdbcDataCaptureService queryService = getQueryService(service);
    StatementParameterList parameters = queryService.getStatementParameters();
    assertEquals(BooleanStatementParameter.class, parameters.getParameterByName("b").getClass());
    assertEquals(ShortStatementParameter.class, parameters.getParameterByName("s").getClass());
    assertEquals(IntegerStatementParameter.class, parameters.getParameterByName("i").getClass());
    assertEquals(LongStatementParameter.class, parameters.getParameterByName("l").getClass());
    assertEquals(FloatStatementParameter.class, parameters.getParameterByName("f").getClass());
    assertEquals(DoubleStatementParameter.class, parameters.getParameterByName("d").getClass());
    assertEquals(StringStatementParameter.class, parameters.getParameterByName("r").getClass());
    assertEquals(TimeStatementParameter.class, parameters.getParameterByName("t").getClass());
    assertEquals(DateStatementParameter.class, parameters.getParameterByName("a").getClass());
    assertEquals(TimestampStatementParameter.class, parameters.getParameterByName("m").getClass());
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
