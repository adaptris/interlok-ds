package com.adaptris.interlok.jdbc;

import com.adaptris.core.services.jdbc.JdbcDataQueryService;
import com.adaptris.core.services.jdbc.StatementParameterList;
import com.adaptris.core.services.jdbc.StringStatementParameter;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class JDBCDataQueryStatementBuilderTest extends JDBCStatementBuilderCase
{
  @Test
  public void testCreateService() throws Exception
  {
    JDBCDataQueryStatementBuilderService service = getService();

    JdbcDataQueryService queryService = getQueryService(service);
    StatementParameterList parameters = queryService.getStatementParameters();
    assertEquals(StringStatementParameter.class, parameters.getParameterByName("id").getClass());
  }

  @Test
  public void testInvalidTypes()
  {
    try
    {
      JDBCDataQueryStatementBuilderService service = new JDBCDataQueryStatementBuilderService();
      service.setStatement("SELECT * FROM person WHERE id = %sql_invalid{invalid:id}");
      service.initService();
    }
    catch (Exception e)
    {
      // expected
    }
  }

  private static JdbcDataQueryService getQueryService(JDBCDataQueryStatementBuilderService service) throws Exception
  {
    Class c = JDBCDataQueryStatementBuilderService.class;
    Field f = c.getDeclaredField("service");
    f.setAccessible(true);
    return (JdbcDataQueryService)f.get(service);
  }

  protected static JDBCDataQueryStatementBuilderService getService()
  {
    try
    {
      JDBCDataQueryStatementBuilderService service = new JDBCDataQueryStatementBuilderService();
      service.setStatement("SELECT * FROM person WHERE id = %sql_payload{string:id}");
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
