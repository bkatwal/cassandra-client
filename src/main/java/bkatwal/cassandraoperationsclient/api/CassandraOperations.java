package bkatwal.cassandraoperationsclient.api;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/** @author "Bikas Katwal" 07/09/18 */
public interface CassandraOperations extends Serializable {

  boolean saveToStage(final String schema, final String targetTableName, final String jsonData);

  void saveToStageAsync(String keySpace, String table, String jsonData);

  void saveToStageAsyncBatch(
      final String keySpace, final String table, final Collection<String> jsonDataList);

  boolean deleteAll(final String schema, final String targetTableName);

  List<Map<String, Object>> getByQuery(
      final String schema, final String targetTableName, final String query);

  List<Map<String, Object>> getByTokenRange(
      final String keySpace, final String table, final Object[] tokenRange);

  List<Map<String, Object>> getByField(
      final String keySpace, final String table, final String fieldName, final Object fieldValue);
}
