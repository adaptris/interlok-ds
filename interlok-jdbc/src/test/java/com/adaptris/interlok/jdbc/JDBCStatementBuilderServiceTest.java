package com.adaptris.interlok.jdbc;

import com.adaptris.core.services.jdbc.DateStatementParameter;
import com.adaptris.core.services.jdbc.IntegerStatementParameter;
import com.adaptris.core.services.jdbc.JdbcDataQueryService;
import com.adaptris.core.services.jdbc.LongStatementParameter;
import com.adaptris.core.services.jdbc.StatementParameterList;
import com.adaptris.core.services.jdbc.StringStatementParameter;
import com.adaptris.interlok.junit.scaffolding.services.JdbcServiceCase;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class JDBCStatementBuilderServiceTest extends JdbcServiceCase
{

  @Test
  public void test() throws Exception
  {
    JDBCStatementBuilderService service = new JDBCStatementBuilderService();
    service.setStatement("INSERT INTO hits (id, name, dob, age) VALUES (" +
        "%sql_id{integer:id}, " +
        "%sql_metadata{string:name}, " +
        "%sql_metadata{date:dob}, " +
        "%sql_metadata{long:age});");
    service.initService();

    JdbcDataQueryService queryService = getQueryService(service);
    StatementParameterList parameters = queryService.getStatementParameters();
    assertEquals(IntegerStatementParameter.class, parameters.getParameterByName("id").getClass());
    assertEquals(StringStatementParameter.class, parameters.getParameterByName("name").getClass());
    assertEquals(DateStatementParameter.class, parameters.getParameterByName("dob").getClass());
    assertEquals(LongStatementParameter.class, parameters.getParameterByName("age").getClass());
  }

  private static JdbcDataQueryService getQueryService(JDBCStatementBuilderService service) throws Exception
  {
    Class c = JDBCStatementBuilderService.class;
    Field f = c.getDeclaredField("service");
    f.setAccessible(true);
    return (JdbcDataQueryService)f.get(service);
  }

  @Override
  protected Object retrieveObjectForSampleConfig()
  {
    JDBCStatementBuilderService service = new JDBCStatementBuilderService();
    service.setStatement("SELECT * FROM *;");
    return service;
  }
}
