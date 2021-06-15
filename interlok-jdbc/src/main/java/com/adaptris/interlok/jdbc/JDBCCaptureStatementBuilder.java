package com.adaptris.interlok.jdbc;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.services.jdbc.JdbcServiceWithParameters;
import com.adaptris.core.services.jdbc.raw.JdbcRawDataCaptureService;
import com.adaptris.core.util.LifecycleHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * JDBC data captuer statement builder service.
 *
 * <p>Build a JDBC data captuere service using the given statement to
 * create the necessary parameter list. It turns:</p>
 *
<pre>
<code>
&lt;jdbc-upsert-service&gt;
    &lt;unique-id&gt;7b18517b-017d-4741-8b5e-9e490bff6c51&lt;/unique-id&gt;
    &lt;statement&gt;INSERT INTO person (id, name, dob, age) VALUES (%sql_id{string:id}, %sql_payload{string:name}, %sql_metadata{date:dob}, %sql_metadata{integer:age})&lt;/statement&gt;
&lt;/jdbc-upsert-service&gt;
</code>
</pre>
 *
 * <p>into:</p>
 *
<pre><code>
&lt;jdbc-raw-data-capture-service&gt;
    &lt;uniqueId&gt;3759673b-e2e2-4f01-a122-b59b67931a02&lt;/uniqueId&gt;
    &lt;named-parameter-applicator&gt;
        &lt;parameterNamePrefix&gt;#&lt;/parameterNamePrefix&gt;
        &lt;parameterNameRegex&gt;#\w*&lt;/parameterNameRegex&gt;
    &lt;/named-parameter-applicator&gt;
    &lt;statementParameters&gt;
        &lt;parameters&gt;
            &lt;jdbc-integer-statement-parameter&gt;
                &lt;name&gt;age&lt;/name&gt;
                &lt;queryString&gt;age&lt;/queryString&gt;
                &lt;queryType&gt;metadata&lt;/queryType&gt;
                &lt;convertNull&gt;false&lt;/convertNull&gt;
            &lt;/jdbc-integer-statement-parameter&gt;
            &lt;jdbc-date-statement-parameter&gt;
                &lt;name&gt;dob&lt;/name&gt;
                &lt;queryString&gt;dob&lt;/queryString&gt;
                &lt;queryType&gt;metadata&lt;/queryType&gt;
                &lt;convertNull&gt;false&lt;/convertNull&gt;
                &lt;dateFormat&gt;yyyy-MM-dd&lt;/dateFormat&gt;
            &lt;/jdbc-date-statement-parameter&gt;
            &lt;jdbc-string-statement-parameter&gt;
                &lt;name&gt;name&lt;/name&gt;
                &lt;queryString&gt;name&lt;/queryString&gt;
                &lt;queryType&gt;payload&lt;/queryType&gt;
                &lt;convertNull&gt;false&lt;/convertNull&gt;
            &lt;/jdbc-string-statement-parameter&gt;
            &lt;jdbc-string-statement-parameter&gt;
                &lt;name&gt;id&lt;/name&gt;
                &lt;queryString&gt;id&lt;/queryString&gt;
                &lt;queryType&gt;id&lt;/queryType&gt;
                &lt;convertNull&gt;false&lt;/convertNull&gt;
            &lt;/jdbc-string-statement-parameter&gt;
        &lt;/parameters&gt;
    &lt;/statementParameters&gt;
    &lt;statement&gt;INSERT INTO person (id, name, dob, age) VALUES (#id, #name, #dob, #age)&lt;/statement&gt;
&lt;/jdbc-raw-data-capture-service&gt;
</code></pre>
 */
@XStreamAlias("jdbc-upsert-service")
@AdapterComponent
@ComponentProfile(summary = "JDBC data capture statement builder service", tag = "jdbc,capture,build,statement", since = "4.1.0")
@DisplayOrder(order = { "connection", "statement" })
public class JDBCCaptureStatementBuilder extends JDBCStatementBuilder
{
  private transient JdbcRawDataCaptureService service;

  @Override
  protected JdbcServiceWithParameters createService(String statement)
  {
    service = new JdbcRawDataCaptureService();
    service.setStatement(statement);
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
}
