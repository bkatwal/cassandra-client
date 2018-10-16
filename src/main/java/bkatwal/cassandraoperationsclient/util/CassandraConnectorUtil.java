package bkatwal.cassandraoperationsclient.util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** @author "Bikas Katwal" 26/08/18 */
public final class CassandraConnectorUtil {

  private static Cluster cluster;

  private static Session session;

  private static final String COMMA_SEPARATOR = ",";

  private CassandraConnectorUtil() {}

  private static Session connect(
      final String node, final Integer port, final String userName, final String password) {
    Builder b =
        Cluster.builder()
            .addContactPoints(node.split(COMMA_SEPARATOR))
            .withCredentials(userName, password);
    if (port != null && port != 0) {
      b.withPort(port);
    }
    cluster = b.build();

    // BKTODO add keysapce here itself
    session = cluster.connect();
    return session;
  }

  public static synchronized Session getSessionObj(final String node, final Integer port) {
    if (session != null) {
      return session;
    } else {
      return connect(node, port, null, null);
    }
  }

  public static synchronized Session getSessionObj(
      final String node, final Integer port, final String userName, final String password) {
    if (session != null) {
      return session;
    } else {
      return connect(node, port, userName, password);
    }
  }

  public static void close() {
    try {
      session.close();
      cluster.close();
    } catch (Exception e) {
      // log here
    }
  }

  private static Host getLocalHost(Metadata metadata, LoadBalancingPolicy policy) {

    Set<Host> allHosts = metadata.getAllHosts();
    Host localHost = null;
    for (Host host : allHosts) {
      if (policy.distance(host) == HostDistance.LOCAL) {
        localHost = host;
        break;
      }
    }
    return localHost;
  }

  public static synchronized List<Object[]> getTokenRangesAcrossNodes(
      final String node,
      final Integer port,
      final String keySpace,
      final String userName,
      final String password) {

    if (cluster == null) {
      connect(node, port, userName, password);
    }
    Metadata metadata = cluster.getMetadata();
    Host localhost =
        getLocalHost(metadata, cluster.getConfiguration().getPolicies().getLoadBalancingPolicy());
    return unwrapTokenRanges(metadata.getTokenRanges(keySpace, localhost));
  }

  private static List<Object[]> unwrapTokenRanges(Set<TokenRange> wrappedRanges) {

    final int tokensSize = 2;
    List<Object[]> tokenRanges = new ArrayList<>();
    for (TokenRange tokenRange : wrappedRanges) {
      List<TokenRange> unwrappedTokenRangeList = tokenRange.unwrap();
      for (TokenRange unwrappedTokenRange : unwrappedTokenRangeList) {
        Object[] objects = new Object[tokensSize];
        objects[0] = unwrappedTokenRange.getStart().getValue();
        objects[1] = unwrappedTokenRange.getEnd().getValue();
        tokenRanges.add(objects);
      }
    }
    return tokenRanges;
  }
}
