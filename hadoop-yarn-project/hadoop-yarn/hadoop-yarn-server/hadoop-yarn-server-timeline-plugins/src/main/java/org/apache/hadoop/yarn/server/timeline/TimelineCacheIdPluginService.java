package org.apache.hadoop.yarn.server.timeline;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * Factory Service to load all of the TimelineCacheIdPlugins
 *
 */
public class TimelineCacheIdPluginService {

  private static TimelineCacheIdPluginService service;
  private ServiceLoader<TimelineCacheIdPlugin> loader;

  private TimelineCacheIdPluginService() {
    loader = ServiceLoader.load(TimelineCacheIdPlugin.class);
  }

  public static synchronized TimelineCacheIdPluginService getInstance() {
    if (service == null) {
      service = new TimelineCacheIdPluginService();
    }
    return service;
  }

  public List<TimelineCacheIdPlugin> getTimelineCacheIdPlugins() {
    List<TimelineCacheIdPlugin> timelineCacheIdPlugins =
        new ArrayList<TimelineCacheIdPlugin>();
    try {
      Iterator<TimelineCacheIdPlugin> plugins = loader.iterator();
      while (plugins.hasNext()) {
        timelineCacheIdPlugins.add(plugins.next());
      }
    } catch (ServiceConfigurationError serviceError) {
      serviceError.printStackTrace();
    }
    return timelineCacheIdPlugins;
  }
}
