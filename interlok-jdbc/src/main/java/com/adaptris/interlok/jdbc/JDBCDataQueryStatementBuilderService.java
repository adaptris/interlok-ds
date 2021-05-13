package com.adaptris.interlok.jdbc;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.services.jdbc.ConfiguredSQLStatement;
import com.adaptris.core.services.jdbc.JdbcDataQueryService;
import com.adaptris.core.services.jdbc.JdbcServiceWithParameters;
import com.adaptris.core.util.LifecycleHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * JDBC data query statement builder service.
 *
 * {@inheritDoc}.
 */
@XStreamAlias("jdbc-data-query-statement-builder-service")
@AdapterComponent
@ComponentProfile(summary = "JDBC data query statement builder service", tag = "service,jdbc", since = "4.1.0")
@DisplayOrder(order = { "connection", "statement" })
public class JDBCDataQueryStatementBuilderService extends JDBCStatementBuilderService
{
	private transient JdbcDataQueryService service;

	@Override
	protected JdbcServiceWithParameters createService(String statement)
	{
		service = new JdbcDataQueryService();
		service.setStatementCreator(new ConfiguredSQLStatement(statement));
		return service;
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

	/**
	 * Prepare for initialisation.
	 *
	 * @throws CoreException
	 */
	@Override
	public void prepareService() throws CoreException
	{
		LifecycleHelper.prepare(service);
	}
}
