package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.CacheId;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.CacheIdProto;

import com.google.common.base.Preconditions;

@Private
@Unstable
public class CacheIdPBImpl extends CacheId{
  CacheIdProto proto = null;
  CacheIdProto.Builder builder = null;
  private ApplicationAttemptId applicationAttemptId = null;

  public CacheIdPBImpl() {
    builder = CacheIdProto.newBuilder();
  }

  public CacheIdPBImpl(CacheIdProto proto) {
    this.proto = proto;
    this.applicationAttemptId = convertFromProtoFormat(proto.getAppAttemptId());
  }
  
  public CacheIdProto getProto() {
    return proto;
  }

  @Override
  public long getCacheId() {
    Preconditions.checkNotNull(proto);
    return proto.getId();
  }

  @Override
  protected void setCacheId(long id) {
    Preconditions.checkNotNull(builder);
    builder.setId((id));
  }


  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return this.applicationAttemptId;
  }

  @Override
  protected void setApplicationAttemptId(ApplicationAttemptId atId) {
    if (atId != null) {
      Preconditions.checkNotNull(builder);
      builder.setAppAttemptId(convertToProtoFormat(atId));
    }
    this.applicationAttemptId = atId;
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(
      ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(
      ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl)t).getProto();
  }

  @Override
  protected void build() {
    proto = builder.build();
    builder = null;
  }
}
