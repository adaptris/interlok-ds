package com.adaptris.interlok.jdbc;

import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.constraints.NotBlank;

import com.adaptris.core.CoreException;
import com.adaptris.core.DefaultMarshaller;
import com.adaptris.core.jdbc.JdbcService;
import com.adaptris.core.services.jdbc.BooleanStatementParameter;
import com.adaptris.core.services.jdbc.DateStatementParameter;
import com.adaptris.core.services.jdbc.DoubleStatementParameter;
import com.adaptris.core.services.jdbc.FloatStatementParameter;
import com.adaptris.core.services.jdbc.IntegerStatementParameter;
import com.adaptris.core.services.jdbc.JdbcServiceWithParameters;
import com.adaptris.core.services.jdbc.LongStatementParameter;
import com.adaptris.core.services.jdbc.NamedParameterApplicator;
import com.adaptris.core.services.jdbc.ShortStatementParameter;
import com.adaptris.core.services.jdbc.StatementParameterImpl;
import com.adaptris.core.services.jdbc.StatementParameterList;
import com.adaptris.core.services.jdbc.StringStatementParameter;
import com.adaptris.core.services.jdbc.TimeStatementParameter;
import com.adaptris.core.services.jdbc.TimestampStatementParameter;
import com.adaptris.core.services.jdbc.TypedStatementParameter;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LifecycleHelper;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * Base class for JDBC data statement builder services; turns the given statement into the necessary query/capture service with the
 * necessary parameter list.
 *
 * @author aanderson
 * @since 4.1.0
 */
public abstract class JDBCStatementBuilder extends JdbcService {
  private static final String SQL_PARAMETER_REGEX = "^.*%sql_([a-z]+)\\{([a-z]+):([\\w!\\$\"#&%'\\*\\+,\\-\\.:=]+)\\}.*$";
  private static final transient Pattern sqlParameterResolver = Pattern.compile(SQL_PARAMETER_REGEX, Pattern.DOTALL);

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

  /**
   * Prepare for initialisation.
   *
   * @throws CoreException
   */
  @Override
  public void prepareService() throws CoreException {
    JdbcServiceWithParameters service = buildService(statement);
    log.trace("Wrapped Service : {}", DefaultMarshaller.getDefaultMarshaller().marshal(service));
    LifecycleHelper.prepare(service);
  }

  protected abstract JdbcServiceWithParameters createService(String statement);

  /**
   * Build a JDBC data query service using the given statement to create the necessary parameter list.
   *
   * @param statement
   *          The SQL statement.
   *
   * @return The constructed JDBC query service.
   */
  protected JdbcServiceWithParameters buildService(String statement) throws CoreException {
    try {
      StatementParameterList parameters = new StatementParameterList();

      String result = statement;
      Matcher matcher = sqlParameterResolver.matcher(statement);
      while (matcher.matches()) {
        String from = matcher.group(1);
        String type = matcher.group(2);
        String name = matcher.group(3);

        String toReplace = "%sql_" + from + "{" + type + ":" + name + "}";
        result = result.replace(toReplace, "#" + name);

        TypedStatementParameter<?> parameter = Type.parse(type).newInstance(statement, name, parseQueryType(from));
        parameter.setQueryString(name);
        parameters.add(parameter);

        matcher = sqlParameterResolver.matcher(result);
      }

      log.trace("Converted [{}] into [{}]", statement, result);

      JdbcServiceWithParameters service = createService(result);
      service.setStatementParameters(parameters);
      service.setParameterApplicator(new NamedParameterApplicator());
      service.setConnection(getConnection());
      return service;
    } catch (Exception e) {
      throw ExceptionHelper.wrapCoreException(e);
    }
  }

  private static StatementParameterImpl.QueryType parseQueryType(String s) {
    for (StatementParameterImpl.QueryType q : StatementParameterImpl.QueryType.values()) {
      if (q.name().equalsIgnoreCase(s)) {
        return q;
      }
    }
    return null;
  }

  /*
   * I wonder if these types can come from the JDBC driver or the implementations of TypedStatementParameter<?>, instead of being hardcoded
   * here.
   */
  private enum Type {
    STRING {
      @Override
      TypedStatementParameter<?> newInstance(String query, String name, StatementParameterImpl.QueryType queryType) {
        return new StringStatementParameter(query, queryType, false, name);
      }
    },
    SHORT {
      @Override
      TypedStatementParameter<?> newInstance(String query, String name, StatementParameterImpl.QueryType queryType) {
        return new ShortStatementParameter(query, queryType, false, name);
      }
    },
    INTEGER {
      @Override
      TypedStatementParameter<?> newInstance(String query, String name, StatementParameterImpl.QueryType queryType) {
        return new IntegerStatementParameter(query, queryType, false, name);
      }
    },
    LONG {
      @Override
      TypedStatementParameter<?> newInstance(String query, String name, StatementParameterImpl.QueryType queryType) {
        return new LongStatementParameter(query, queryType, false, name);
      }
    },
    FLOAT {
      @Override
      TypedStatementParameter<?> newInstance(String query, String name, StatementParameterImpl.QueryType queryType) {
        return new FloatStatementParameter(query, queryType, false, name);
      }
    },
    DOUBLE {
      @Override
      TypedStatementParameter<?> newInstance(String query, String name, StatementParameterImpl.QueryType queryType) {
        return new DoubleStatementParameter(query, queryType, false, name);
      }
    },
    BOOLEAN {
      @Override
      TypedStatementParameter<?> newInstance(String query, String name, StatementParameterImpl.QueryType queryType) {
        return new BooleanStatementParameter(query, queryType, false, name);
      }
    },
    DATE {
      @Override
      TypedStatementParameter<?> newInstance(String query, String name, StatementParameterImpl.QueryType queryType) {
        return new DateStatementParameter(query, queryType, false, name, DATE_FORMAT);
      }
    },
    TIME {
      @Override
      TypedStatementParameter<?> newInstance(String query, String name, StatementParameterImpl.QueryType queryType) {
        return new TimeStatementParameter(query, queryType, false, name, TIME_FORMAT);
      }
    },
    TIMESTAMP {
      @Override
      TypedStatementParameter<?> newInstance(String query, String name, StatementParameterImpl.QueryType queryType) {
        return new TimestampStatementParameter(query, queryType, false, name, TIMESTAMP_FORMAT);
      }
    };

    abstract TypedStatementParameter<?> newInstance(String query, String name, StatementParameterImpl.QueryType queryType);

    public static Type parse(String s) {
      for (Type t : Type.values()) {
        if (t.name().equalsIgnoreCase(s)) {
          return t;
        }
      }
      return null;
    }
  }

}
