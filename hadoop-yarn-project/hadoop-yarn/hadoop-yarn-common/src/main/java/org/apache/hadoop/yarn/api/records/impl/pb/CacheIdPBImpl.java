package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.CacheId;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.CacheIdProto;

import com.google.common.base.Preconditions;

@Private
@Unstable
public class CacheIdPBImpl extends CacheId{
  CacheIdProto proto = null;
  CacheIdProto.Builder builder = null;
  private ApplicationId applicationId = null;

  public CacheIdPBImpl() {
    builder = CacheIdProto.newBuilder();
  }

  public CacheIdPBImpl(CacheIdProto proto) {
    this.proto = proto;
    this.applicationId = convertFromProtoFormat(proto.getAppId());
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
  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  @Override
  protected void setApplicationId(ApplicationId appId) {
    if (appId != null) {
      Preconditions.checkNotNull(builder);
      builder.setAppId(convertToProtoFormat(appId));
    }
    this.applicationId = appId;
  }

  private ApplicationIdPBImpl convertFromProtoFormat(
      ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  private ApplicationIdProto convertToProtoFormat(
      ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }

  @Override
  protected void build() {
    proto = builder.build();
    builder = null;
  }
}
