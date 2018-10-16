package bkatwal.cassandraoperationsclient.util;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;
import java.util.Set;

/** @author "Bikas Katwal" 10/09/18 */
public enum CassandraTypeToJavaType {
  BIGINT(Long.class),
  VARCHAR(String.class),
  BOOLEAN(Boolean.class),
  INT(Integer.class),
  SET(Set.class),
  TIMESTAMP(Date.class),
  DECIMAL(BigDecimal.class),
  MAP(Map.class);

  private final Class clazz;

  CassandraTypeToJavaType(Class clazz) {
    this.clazz = clazz;
  }

  public Class clazz() {
    return clazz;
  }
}
