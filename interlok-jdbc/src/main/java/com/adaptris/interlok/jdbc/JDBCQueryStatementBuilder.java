package com.adaptris.interlok.jdbc;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.services.jdbc.ConfiguredSQLStatement;
import com.adaptris.core.services.jdbc.JdbcDataQueryService;
import com.adaptris.core.services.jdbc.JdbcServiceWithParameters;
import com.adaptris.core.services.jdbc.NoOpResultSetTranslator;
import com.adaptris.core.services.jdbc.ResultSetTranslator;
import com.adaptris.core.util.LifecycleHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.ObjectUtils;

/**
 * JDBC data query statement builder service.
 *
 * <p>Build a JDBC data query service using the given statement to
 * create the necessary parameter list. It turns:</p>
 *
<pre>
<code>
&lt;jdbc-query-statement-builder&gt;
    &lt;unique-id&gt;7b18517b-017d-4741-8b5e-9e490bff6c51&lt;/unique-id&gt;
    &lt;statement&gt;SELECT * FROM person WHERE id = %sql_payload{string:id}&lt;/statement&gt;
&lt;/jdbc-query-statement-builder&gt;
</code>
</pre>
 *
 * <p>into:</p>
 *
<pre><code>
&lt;jdbc-data-query-service&gt;
    &lt;uniqueId&gt;e99b6678-da08-4e25-810a-4d2cb59f0a44&lt;/uniqueId&gt;
    &lt;named-parameter-applicator&gt;
        &lt;parameterNamePrefix&gt;#&lt;/parameterNamePrefix&gt;
        &lt;parameterNameRegex&gt;#\w*&lt;/parameterNameRegex&gt;
    &lt;/named-parameter-applicator&gt;
    &lt;statementParameters&gt;
        &lt;parameters&gt;
            &lt;jdbc-string-statement-parameter&gt;
                &lt;name&gt;id&lt;/name&gt;
                &lt;queryString&gt;id&lt;/queryString&gt;
                &lt;queryType&gt;payload&lt;/queryType&gt;
                &lt;convertNull&gt;false&lt;/convertNull&gt;
            &lt;/jdbc-string-statement-parameter&gt;
        &lt;/parameters&gt;
    &lt;/statementParameters&gt;
    &lt;jdbc-configured-sql-statement&gt;
        &lt;statement&gt;SELECT * FROM person WHERE id = #id&lt;/statement&gt;
    &lt;/jdbc-configured-sql-statement&gt;
    &lt;jdbc-noop-result-set-translator/&gt;
&lt;/jdbc-data-query-service&gt;
</code></pre>
 */
@XStreamAlias("jdbc-query-statement-builder")
@AdapterComponent
@ComponentProfile(summary = "JDBC data query statement builder service", tag = "jdbc,query,build,statement", since = "4.1.0")
@DisplayOrder(order = { "connection", "statement" })
public class JDBCQueryStatementBuilder extends JDBCStatementBuilder
{
  @AutoPopulated
  @Getter
  @Setter
  @AdvancedConfig
  private ResultSetTranslator resultSetTranslator;

  private transient JdbcDataQueryService service;

  @Override
  protected JdbcServiceWithParameters createService(String statement)
  {
    service = new JdbcDataQueryService();
    service.setStatementCreator(new ConfiguredSQLStatement(statement));
    service.setResultSetTranslator(resultSetTranslator());
    return service;
  }

  @Override
  protected void initJdbcService() throws CoreException
  {
    LifecycleHelper.init(service);
  }

  /**
   * Close the service.
   * <p>
   * This is called before the connection is closed
   * </p>
   */
  @Override
  protected void closeJdbcService()
  {
    LifecycleHelper.close(service);
  }

  /**
   * Start the service.
   * <p>
   * This is called after the connection is started
   * </p>
   *
   * @throws CoreException
   */
  @Override
  protected void startService() throws CoreException
  {
    LifecycleHelper.start(service);
  }

  /**
   * Stop the service.
   * <p>
   * This is called after before the connection is stopped
   * </p>
   */
  @Override
  protected void stopService()
  {
    LifecycleHelper.stop(service);
  }

  /**
   * Apply the service to the message.
   *
   * @param message the <code>AdaptrisMessage</code> to process.
   * @throws ServiceException wrapping any underlying <code>Exception</code>.
   */
  @Override
  public void doService(AdaptrisMessage message) throws ServiceException
  {
    service.doService(message);
  }

  private ResultSetTranslator resultSetTranslator()
  {
    return ObjectUtils.defaultIfNull(resultSetTranslator, new NoOpResultSetTranslator());
  }
}
