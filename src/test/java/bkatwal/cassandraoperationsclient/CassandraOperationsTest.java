package bkatwal.cassandraoperationsclient;

import bkatwal.cassandraoperationsclient.api.CassandraOperations;
import bkatwal.cassandraoperationsclient.impl.CassandraOperationsImpl;
import bkatwal.cassandraoperationsclient.util.CassandraConnectorUtil;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author "Bikas Katwal" 10/09/18 */
public class CassandraOperationsTest {

  private static CassandraOperations cassandraOperations;

  private static Session session;

  private static String keySpace = "test_key_space";

  private static String tableName = "test_table";

  private static String tableName2 = "test_table2";

  private static int port;
  private static String host;

  private static final int UPDATE_SIZE_CASSANDRA = 3000;

  private static final int READ_TIMEOUT_CASSANDRA = 15;

  @AfterClass
  public static void destroy() {

    CassandraConnectorUtil.close();
    // EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    // EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
  }

  @BeforeClass
  public static void init() throws IOException, TTransportException {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(
        EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);

    port = EmbeddedCassandraServerHelper.getNativeTransportPort();
    host = EmbeddedCassandraServerHelper.getHost();
    cassandraOperations =
        new CassandraOperationsImpl(host, port, UPDATE_SIZE_CASSANDRA, READ_TIMEOUT_CASSANDRA);
    session = ((CassandraOperationsImpl) cassandraOperations).session;
    session.execute(
        "CREATE KEYSPACE test_key_space WITH replication "
            + "= {'class':'SimpleStrategy', 'replication_factor':1};");
    session.execute(
        "CREATE TABLE test_key_space.test_table (\n"
            + "    id text PRIMARY KEY,\n"
            + "  \tdecimalField decimal,\n"
            + "    booleanField boolean,\n"
            + "    textField text,\n"
            + "    timestampField timestamp,\n"
            + "    setField set<text>,\n"
            + "    intField int,\n"
            + "    bigintField bigint,\n"
            + "\n"
            + "    );\n"
            + "\n");

    session.execute(
        "CREATE TABLE test_key_space.test_table2 (\n"
            + "    id text PRIMARY KEY,\n"
            + "  \tdecimalField decimal,\n"
            + "    booleanField boolean,\n"
            + "    textField text,\n"
            + "    timestampField timestamp,\n"
            + "    setField set<text>,\n"
            + "    intField int,\n"
            + "    bigintField bigint,\n"
            + "\n"
            + "    );\n"
            + "\n");
  }

  @Test
  public void testSaveData() {

    String json =
        "{\n"
            + "  \"id\": \"9999\",\n"
            + "  \"decimalField\": 123.12,\n"
            + "  \"booleanField\": true,\n"
            + "  \"textField\": \"dasd\",\n"
            + "  \"timestampField\": null,\n"
            + "  \"setField\": [\n"
            + "    \"a\",\n"
            + "    \"b\",\n"
            + "    \"c\"\n"
            + "  ],\n"
            + "  \"intField\": 12,\n"
            + "  \"bigintField\": 12\n"
            + "}";
    cassandraOperations.saveToStage(keySpace, tableName, json);

    ResultSet rows = session.execute("select * from test_key_space.test_table where id = '9999'");
    Assert.assertEquals(rows.all().size(), 1);
  }

  @Test
  public void testSaveDataAsync() throws InterruptedException {

    String json =
        "{\n"
            + "  \"id\": \"9997\",\n"
            + "  \"decimalField\": 123.12,\n"
            + "  \"booleanField\": true,\n"
            + "  \"textField\": \"dasd\",\n"
            + "  \"timestampField\": null,\n"
            + "  \"setField\": [\n"
            + "    \"a\",\n"
            + "    \"b\",\n"
            + "    \"c\"\n"
            + "  ],\n"
            + "  \"intField\": 12,\n"
            + "  \"bigintField\": 12\n"
            + "}";
    cassandraOperations.saveToStageAsync(keySpace, tableName, json);

    Thread.sleep(200L);
    ResultSet rows = session.execute("select * from test_key_space.test_table where id = '9997'");
    Assert.assertEquals(rows.all().size(), 1);
  }

  @Test
  public void getByTokenRangeTest() {
    loadTestData();
    List<Object[]> tokenRanges =
        CassandraConnectorUtil.getTokenRangesAcrossNodes(host, port, null, null);

    List<Map<String, Object>> records = new ArrayList<>();
    for (Object[] tokenRange : tokenRanges) {
      records.addAll(cassandraOperations.getByTokenRange(keySpace, tableName2, tokenRange));
    }

    Assert.assertEquals(records.size(), 100);
  }

  @Test
  public void getByFieldTest() {
    loadTestData();

    List<Map<String, Object>> records =
        new ArrayList<>(cassandraOperations.getByField(keySpace, tableName2, "id", "2"));

    Assert.assertEquals(records.size(), 1);
  }

  private static void loadTestData() {

    for (Integer i = 0; i < 100; i++) {
      List<String> names = new ArrayList<>();
      names.add("id");
      names.add("decimalField");
      names.add("booleanField");
      names.add("textField");
      names.add("timestampField");
      names.add("setField");
      names.add("intField");
      names.add("bigintField");

      List<Object> values = new ArrayList<>();
      values.add(i + "");
      values.add(new BigDecimal(i));
      values.add(true);
      values.add("text");
      values.add(new Date());
      values.add(new HashSet<>(Arrays.asList("1", "2", "3")));
      values.add(i * 10);
      values.add(999999 * i);

      Statement stmt = QueryBuilder.insertInto(keySpace, tableName2).values(names, values);
      session.execute(stmt);
    }
  }
}
