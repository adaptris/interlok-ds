package com.adaptris.interlok.jdbc;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.services.jdbc.BooleanStatementParameter;
import com.adaptris.core.services.jdbc.ConfiguredSQLStatement;
import com.adaptris.core.services.jdbc.DateStatementParameter;
import com.adaptris.core.services.jdbc.DoubleStatementParameter;
import com.adaptris.core.services.jdbc.FloatStatementParameter;
import com.adaptris.core.services.jdbc.IntegerStatementParameter;
import com.adaptris.core.services.jdbc.JdbcDataQueryService;
import com.adaptris.core.services.jdbc.JdbcStatementParameter;
import com.adaptris.core.services.jdbc.LongStatementParameter;
import com.adaptris.core.services.jdbc.ShortStatementParameter;
import com.adaptris.core.services.jdbc.StatementParameterImpl;
import com.adaptris.core.services.jdbc.StatementParameterList;
import com.adaptris.core.services.jdbc.StringStatementParameter;
import com.adaptris.core.services.jdbc.TimeStatementParameter;
import com.adaptris.core.services.jdbc.TimestampStatementParameter;
import com.adaptris.core.services.jdbc.TypedStatementParameter;
import com.adaptris.core.util.LifecycleHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import javax.validation.constraints.NotBlank;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@XStreamAlias("jdbc-statement-builder-service")
@AdapterComponent
@ComponentProfile(summary = "Do something JDBC-ish", tag = "service,jdbc")
@DisplayOrder(order = { "statement" })
public class JDBCStatementBuilderService extends ServiceImp
{
  private static final String SQL_PARAMETER_REGEX = "^.*%sql_([a-z]+)\\{([a-z]+):([\\w!\\$\"#&%'\\*\\+,\\-\\.:=]+)\\}.*$";
  private static final transient Pattern sqlParameterResolver = Pattern.compile(SQL_PARAMETER_REGEX);

  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ssZ");
  private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss");

  /**
   * The SQL statement.
   */
  @Getter
  @Setter
  @NotBlank
  @NonNull
  private String statement;

  private transient JdbcDataQueryService service;

  @Override
  protected void initService() throws CoreException
  {
    service = buildTheService(statement);
    LifecycleHelper.init(service);
  }

  @Override
  protected void closeService()
  {
    LifecycleHelper.close(service);
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
  public void prepare() throws CoreException
  {
    LifecycleHelper.prepare(service);
  }

  protected JdbcDataQueryService buildTheService(String statement) {

    JdbcDataQueryService service = new JdbcDataQueryService();
    StatementParameterList parameters = new StatementParameterList();

    String result = statement;
    Matcher matcher = sqlParameterResolver.matcher(statement);
    while (matcher.matches()) {
      String from = matcher.group(1);
      String type = matcher.group(2);
      String name = matcher.group(3);

      log.debug("From = " + from);
      log.debug("Type = " + type);
      log.debug("Name = " + name);

      TypedStatementParameter parameter = Type.parse(type).newInstance(statement, name, parseQueryType(from));

      String toReplace = "%sql_" + from + "{" + type + ":" + name + "}";
      result = result.replace(toReplace, "#" + name);

      parameters.add(parameter);

      matcher = sqlParameterResolver.matcher(result);
    }

    for (JdbcStatementParameter parameter : parameters)
    {
      ((StatementParameterImpl)parameter).setQueryString(result);
    }

    ConfiguredSQLStatement statementCreator = new ConfiguredSQLStatement();
    statementCreator.setStatement(statement);
    service.setStatementCreator(statementCreator);
    service.setStatementParameters(parameters);
    return service;
  }

  private static StatementParameterImpl.QueryType parseQueryType(String s)
  {
    for (StatementParameterImpl.QueryType q : StatementParameterImpl.QueryType.values())
    {
      if (q.name().equalsIgnoreCase(s))
      {
        return q;
      }
    }
    return null;
  }

  private enum Type
  {
    STRING
    {
      @Override
      TypedStatementParameter newInstance(String query, String name, StatementParameterImpl.QueryType queryType)
      {
        return new StringStatementParameter(query, queryType, false, name);
      }
    },
    SHORT
    {
      @Override
      TypedStatementParameter newInstance(String query, String name, StatementParameterImpl.QueryType queryType)
      {
        return new ShortStatementParameter(query, queryType, false, name);
      }
    },
    INTEGER
    {
      @Override
      TypedStatementParameter newInstance(String query, String name, StatementParameterImpl.QueryType queryType)
      {
        return new IntegerStatementParameter(query, queryType, false, name);
      }
    },
    LONG
    {
      @Override
      TypedStatementParameter newInstance(String query, String name, StatementParameterImpl.QueryType queryType)
      {
        return new LongStatementParameter(query, queryType, false, name);
      }
    },
    FLOAT
    {
      @Override
      TypedStatementParameter newInstance(String query, String name, StatementParameterImpl.QueryType queryType)
      {
        return new FloatStatementParameter(query, queryType, false, name);
      }
    },
    DOUBLE
    {
      @Override
      TypedStatementParameter newInstance(String query, String name, StatementParameterImpl.QueryType queryType)
      {
        return new DoubleStatementParameter(query, queryType, false, name);
      }
    },
    BOOLEAN
    {
      @Override
      TypedStatementParameter newInstance(String query, String name, StatementParameterImpl.QueryType queryType)
      {
        return new BooleanStatementParameter(query, queryType, false, name);
      }
    },
    DATE
    {
      @Override
      TypedStatementParameter newInstance(String query, String name, StatementParameterImpl.QueryType queryType)
      {
        return new DateStatementParameter(query, queryType, false, name, DATE_FORMAT);
      }
    },
    TIME
    {
      @Override
      TypedStatementParameter newInstance(String query, String name, StatementParameterImpl.QueryType queryType)
      {
        return new TimeStatementParameter(query, queryType, false, name, TIME_FORMAT);
      }
    },
    TIMESTAMP
    {
      @Override
      TypedStatementParameter newInstance(String query, String name, StatementParameterImpl.QueryType queryType)
      {
        return new TimestampStatementParameter(query, queryType, false, name, TIMESTAMP_FORMAT);
      }
    };

    abstract TypedStatementParameter newInstance(String query, String name, StatementParameterImpl.QueryType queryType);

    public static Type parse(String s)
    {
      for (Type t : Type.values())
      {
        if (t.name().equalsIgnoreCase(s))
        {
          return t;
        }
      }
      return null;
    }
  }
}
