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
  public static CacheId newInstance(ApplicationId applicationId,
      long id) {
    CacheId cacheId = Records.newRecord(CacheId.class);
    cacheId.setApplicationId(applicationId);
    cacheId.setCacheId(id);
    cacheId.build();
    return cacheId;
  }

  /**
   * Get the <code>ApplicationId</code> of the <code>CacheId</code>.
   * @return <code>ApplicationId</code> of the <code>CacheId</code>
   */
  @Public
  @Stable
  public abstract ApplicationId getApplicationId();

  @Private
  @Unstable
  protected abstract void setApplicationId(ApplicationId appID);

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
    result = 31 * result + getApplicationId().hashCode();
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
    if (!this.getApplicationId().equals(other.getApplicationId()))
      return false;
    if (this.getCacheId() != other.getCacheId())
      return false;
    return true;
  }

  @Override
  public int compareTo(CacheId other) {
    int compareAppIds = this.getApplicationId().compareTo(
        other.getApplicationId());
    if (compareAppIds == 0) {
      return Long.valueOf(this.getCacheId()).compareTo(
        Long.valueOf(other.getCacheId()));
    } else {
      return compareAppIds;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(cacheIdStrPrefix + "_");
    ApplicationId appId = getApplicationId();
    sb.append(appId.getClusterTimestamp()).append("_");
    sb.append(appId.getId()).append("_");
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
    return CacheId.newInstance(appId, Long.parseLong(it.next()));
  }

  protected abstract void build();

}
