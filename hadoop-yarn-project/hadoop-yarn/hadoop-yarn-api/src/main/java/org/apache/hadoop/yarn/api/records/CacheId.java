package org.apache.hadoop.yarn.api.records;

import java.util.Iterator;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.base.Splitter;


@Public
@Unstable
public abstract class CacheId implements Comparable<CacheId>{

  private static final Splitter _SPLITTER = Splitter.on('_').trimResults();

  @Private
  @Unstable
  public static final String cacheIdStrPrefix = "cache";

  @Private
  @Unstable
  public static CacheId newInstance(ApplicationAttemptId attemptId,
      long id) {
    CacheId cacheId = Records.newRecord(CacheId.class);
    cacheId.setApplicationAttemptId(attemptId);
    cacheId.setCacheId(id);
    cacheId.build();
    return cacheId;
  }

  /**
   * Get the <code>ApplicationAttemptId</code> of the <code>CacheId</code>. 
   * @return <code>ApplicationAttemptId</code> of the <code>CacheId</code>
   */
  @Public
  @Stable
  public abstract ApplicationAttemptId getApplicationAttemptId();
  
  @Private
  @Unstable
  protected abstract void setApplicationAttemptId(ApplicationAttemptId appID);
  
  /**
   * Get the <code>cache id</code>
   * @return <code>cache id</code>
   */
  @Public
  @Stable
  public abstract long getCacheId();
  
  @Private
  @Unstable
  protected abstract void setCacheId(long cacheId);

  @Override
  public int hashCode() {
    int result = (int) (getCacheId() ^ (getCacheId() >>> 32));
    result = 31 * result + getApplicationAttemptId().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    CacheId other = (CacheId) obj;
    if (!this.getApplicationAttemptId().equals(other.getApplicationAttemptId()))
      return false;
    if (this.getCacheId() != other.getCacheId())
      return false;
    return true;
  }

  @Override
  public int compareTo(CacheId other) {
    int compareAppAttemptIds = this.getApplicationAttemptId().compareTo(
        other.getApplicationAttemptId());
    if (compareAppAttemptIds == 0) {
      return Long.valueOf(this.getCacheId()).compareTo(
        Long.valueOf(other.getCacheId()));
    } else {
      return compareAppAttemptIds;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(cacheIdStrPrefix + "_");
    ApplicationId appId = getApplicationAttemptId().getApplicationId();
    sb.append(appId.getClusterTimestamp()).append("_");
    sb.append(appId.getId()).append("_");
    sb.append(getApplicationAttemptId().getAttemptId()).append("_");
    sb.append(getCacheId());
    return sb.toString();
  }

  public static CacheId fromString(String cacheIdStr) {
    Iterator<String> it = _SPLITTER.split(cacheIdStr).iterator();
    if (!it.next().equals(cacheIdStrPrefix)) {
      throw new IllegalArgumentException("Invalid CacheId prefix: "
          + cacheIdStr);
    }
    ApplicationId appId =
        ApplicationId.newInstance(Long.parseLong(it.next()),
          Integer.parseInt(it.next()));
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, Integer.parseInt(it.next()));
    return CacheId.newInstance(appAttemptId, Long.parseLong(it.next()));
  }

  protected abstract void build();

}
