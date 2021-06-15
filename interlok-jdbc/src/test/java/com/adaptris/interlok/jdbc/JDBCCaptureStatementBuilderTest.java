package com.adaptris.interlok.jdbc;

import com.adaptris.core.services.jdbc.BooleanStatementParameter;
import com.adaptris.core.services.jdbc.DateStatementParameter;
import com.adaptris.core.services.jdbc.DoubleStatementParameter;
import com.adaptris.core.services.jdbc.FloatStatementParameter;
import com.adaptris.core.services.jdbc.IntegerStatementParameter;
import com.adaptris.core.services.jdbc.LongStatementParameter;
import com.adaptris.core.services.jdbc.ShortStatementParameter;
import com.adaptris.core.services.jdbc.StatementParameterList;
import com.adaptris.core.services.jdbc.StringStatementParameter;
import com.adaptris.core.services.jdbc.TimeStatementParameter;
import com.adaptris.core.services.jdbc.TimestampStatementParameter;
import com.adaptris.core.services.jdbc.raw.JdbcRawDataCaptureService;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class JDBCCaptureStatementBuilderTest extends JDBCStatementBuilderCase
{
  @Test
  public void testCreateService() throws Exception
  {
    JDBCCaptureStatementBuilder service = new JDBCCaptureStatementBuilder();
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
    service.prepare();

    JdbcRawDataCaptureService queryService = getQueryService(service);
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

  private static JdbcRawDataCaptureService getQueryService(JDBCCaptureStatementBuilder service) throws Exception
  {
    Class c = JDBCCaptureStatementBuilder.class;
    Field f = c.getDeclaredField("service");
    f.setAccessible(true);
    return (JdbcRawDataCaptureService)f.get(service);
  }

  protected static JDBCCaptureStatementBuilder getService()
  {
    try
    {
      JDBCCaptureStatementBuilder service = new JDBCCaptureStatementBuilder();
      service.setStatement("INSERT INTO person (id, name, dob, age) VALUES (" +
              "%sql_id{string:id}, " +
              "%sql_payload{string:name}, " +
              "%sql_metadata{date:dob}, " +
              "%sql_metadata{integer:age})");
      service.prepare();
      return service;
    }
    catch (Exception e)
    {
      return null;
    }
  }

  @Override
  protected Object retrieveObjectForSampleConfig()
  {
    return getService();
  }
}
