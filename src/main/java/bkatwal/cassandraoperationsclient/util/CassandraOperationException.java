package bkatwal.cassandraoperationsclient.util;

/** @author "Bikas Katwal" 16/10/18 */
public class CassandraOperationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public CassandraOperationException(String message, Throwable cause) {
    super(message, cause);
  }

  public CassandraOperationException(String message) {
    super(message);
  }

  public CassandraOperationException(Throwable cause) {
    super(cause);
  }

  public CassandraOperationException() {
    super();
  }
}
