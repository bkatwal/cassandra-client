package bkatwal.cassandraoperationsclient.impl;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.token;

import bkatwal.cassandraoperationsclient.api.CassandraOperations;
import bkatwal.cassandraoperationsclient.util.CassandraConnectorUtil;
import bkatwal.cassandraoperationsclient.util.CassandraOperationException;
import bkatwal.cassandraoperationsclient.util.CassandraTypeToJavaType;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** @author "Bikas Katwal" 26/08/18 */
public class CassandraOperationsImpl implements CassandraOperations {

  private static final long serialVersionUID = 4713001414763221148L;
  public transient Session session;

  private int updateSize;

  private int readTimeoutInSeconds;

  private static int successCount;

  // this will shared across threads
  private static List<ResultSetFuture> tasks = new LinkedList<>();

  private static final String ID_S = "id";

  public CassandraOperationsImpl(
      final String host, final int port, final int updateSize, final int readTimeoutInSeconds) {
    this.session = CassandraConnectorUtil.getSessionObj(host, port);
    this.updateSize = updateSize;
    this.readTimeoutInSeconds = readTimeoutInSeconds;
  }

  // BKTODO use builder pattern
  public CassandraOperationsImpl(
      final String host,
      final int port,
      final String userName,
      final String password,
      final int updateSize,
      final int readTimeoutInSeconds) {
    this.session = CassandraConnectorUtil.getSessionObj(host, port, userName, password);
    this.updateSize = updateSize;
    this.readTimeoutInSeconds = readTimeoutInSeconds;
  }

  @Override
  public boolean saveToStage(final String keySpace, final String table, final String jsonData) {
    Insert insertQuery = QueryBuilder.insertInto(keySpace, table).json(jsonData);
    ResultSet resultSet = session.execute(insertQuery.toString());
    return resultSet.wasApplied();
  }

  @Override
  public void saveToStageAsync(final String keySpace, final String table, final String jsonData) {
    Insert insertQuery = QueryBuilder.insertInto(keySpace, table).json(jsonData);
    ResultSetFuture future = session.executeAsync(insertQuery.toString());
    Futures.addCallback(future, new AsyncCallback());
  }

  @Override
  public void saveToStageAsyncBatch(
      final String keySpace, final String table, final Collection<String> jsonDataList) {

    try {
      for (String json : jsonDataList) {
        Insert insertQuery = QueryBuilder.insertInto(keySpace, table).json(json);
        ResultSetFuture future = session.executeAsync(insertQuery.toString());
        Futures.addCallback(future, new AsyncCallback());
        tasks.add(future);

        if (tasks.size() < updateSize) {
          continue;
        }

        for (ResultSetFuture t : tasks) {
          t.getUninterruptibly(readTimeoutInSeconds, TimeUnit.SECONDS);
        }
        tasks.clear();
      }
      if (!tasks.isEmpty()) {
        for (ResultSetFuture t : tasks) {
          t.getUninterruptibly(readTimeoutInSeconds, TimeUnit.SECONDS);
        }
      }
    } catch (Exception e) {
      throw new CassandraOperationException("Failed to do async save to cassandra.", e);
    }
  }

  @Override
  public boolean deleteAll(String schema, String targetTableName) {

    Statement statement = QueryBuilder.truncate(schema, targetTableName);
    ResultSet resultSet = session.execute(statement);
    return resultSet.wasApplied();
  }

  @Override
  public List<Map<String, Object>> getByQuery(
      final String keySpace, final String table, final String query) {

    return Collections.emptyList();
  }

  @Override
  public List<Map<String, Object>> getByTokenRange(
      final String keySpace, final String table, final Object[] tokenRange) {

    Statement stmt =
        QueryBuilder.select()
            .all()
            .from(keySpace, table)
            .where(gt(token(ID_S), tokenRange[0]))
            .and(lte(token(ID_S), tokenRange[1]));

    return fetchResult(stmt);
  }

  @Override
  public List<Map<String, Object>> getByField(
      final String keySpace, final String table, final String fieldName, final Object fieldValue) {
    Statement stmt =
        QueryBuilder.select().all().from(keySpace, table).where(eq(fieldName, fieldValue));
    return fetchResult(stmt);
  }

  private List<Map<String, Object>> fetchResult(Statement stmt) {

    ResultSet resultSet = session.execute(stmt);

    List<Row> rows = resultSet.all();

    List<Map<String, Object>> responseList = new ArrayList<>();

    Map<String, Object> rowMap;
    for (Row row : rows) {

      rowMap = new HashMap<>();

      List<Definition> columns = row.getColumnDefinitions().asList();

      for (Definition definition : columns) {
        Class clazz = getJavaType(definition.getType());
        String columnName = definition.getName();

        if (clazz == Set.class) {
          Class childClazz = getJavaType((definition.getType()).getTypeArguments().get(0));
          rowMap.put(definition.getName(), row.getSet(columnName, childClazz));
        } else if (clazz == Map.class) {
          Class keyClass = getJavaType(definition.getType().getTypeArguments().get(0));
          Class valueClass = getJavaType(definition.getType().getTypeArguments().get(1));

          rowMap.put(
              definition.getName(),
              row.getMap(columnName, TypeToken.of(keyClass), TypeToken.of(valueClass)));
        } else {
          rowMap.put(definition.getName(), row.get(columnName, clazz));
        }
      }
      responseList.add(rowMap);
    }

    return responseList;
  }

  private Class getJavaType(DataType type) {
    return CassandraTypeToJavaType.valueOf(type.getName().name()).clazz();
  }

  private static class AsyncCallback implements FutureCallback<ResultSet> {

    @Override
    public void onSuccess(ResultSet result) {
      //Log here
    }

    @Override
    public void onFailure(Throwable t) {
      throw new CassandraOperationException("failed to save record in cassandra!", t);
    }
  }
}
