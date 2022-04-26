/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.DefaultFileSystemMaster.Metrics;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.NoopUfsAbsentPathCache;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UfsStatusCache;
import alluxio.underfs.UnderFileSystem;

import com.codahale.metrics.Counter;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link DefaultFileSystemMaster.Metrics}.
 */
public class FileSystemMasterMetricsTest {

  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  private UfsManager mUfsManager;
  private InodeTree mInodeTree;

  @Before
  public void before() throws Exception {
    MetricsSystem.clearAllMetrics();
    mUfsManager = Mockito.mock(UfsManager.class);
    mInodeTree = Mockito.mock(InodeTree.class);
    Metrics.registerGauges(mUfsManager, mInodeTree);
  }

  @Test
  public void testMetricsFilesPinned() {
    when(mInodeTree.getPinnedSize()).thenReturn(100);
    assertEquals(100, getGauge(MetricKey.MASTER_FILES_PINNED.getName()));
  }

  @Test
  public void testMetricsPathsTotal() {
    when(mInodeTree.getInodeCount()).thenReturn(90L);
    assertEquals(90L, getGauge(MetricKey.MASTER_TOTAL_PATHS.getName()));
  }

  @Test
  public void testMetricsUfsCapacity() throws Exception {
    UfsManager.UfsClient client = Mockito.mock(UfsManager.UfsClient.class);
    UnderFileSystem ufs = Mockito.mock(UnderFileSystem.class);
    String ufsDataFolder = ServerConfiguration.getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    when(ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_TOTAL)).thenReturn(1000L);
    when(ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_USED)).thenReturn(200L);
    when(ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_FREE)).thenReturn(800L);
    when(client.acquireUfsResource()).thenReturn(new CloseableResource<UnderFileSystem>(ufs) {
      @Override
      public void closeResource() {
      }
    });
    when(mUfsManager.getRoot()).thenReturn(client);
    assertEquals(1000L, getGauge(MetricKey.CLUSTER_ROOT_UFS_CAPACITY_TOTAL.getName()));
    assertEquals(200L, getGauge(MetricKey.CLUSTER_ROOT_UFS_CAPACITY_USED.getName()));
    assertEquals(800L, getGauge(MetricKey.CLUSTER_ROOT_UFS_CAPACITY_FREE.getName()));
  }

  @Test
  public void testMetricsUfsStatusCache() throws Exception {
    final Counter cacheSizeTotal = getCounter(MetricKey.MASTER_UFS_STATUS_CACHE_SIZE.getName());
    final Counter cacheChildrenSizeTotal =
        getCounter(MetricKey.MASTER_UFS_STATUS_CACHE_CHILDREN_SIZE.getName());

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    UfsStatusCache ufsStatusCache =
        new UfsStatusCache(executorService, new NoopUfsAbsentPathCache(),
            UfsAbsentPathCache.ALWAYS);

    AlluxioURI path = new AlluxioURI("/dir");
    UfsStatus stat = createUfsStatusWithName("dir");
    UfsStatus statMismatch = createUfsStatusWithName("abc");

    ufsStatusCache.addStatus(path, stat);
    assertEquals(1, cacheSizeTotal.getCount());
    //add once more
    ufsStatusCache.addStatus(path, stat);
    assertEquals(1, cacheSizeTotal.getCount());
    //path and status name mismatch
    assertThrows(IllegalArgumentException.class,
        () -> ufsStatusCache.addStatus(path, statMismatch));
    assertEquals(1, cacheSizeTotal.getCount());

    List<UfsStatus> statusList = ImmutableList.of("1", "2", "3")
        .stream()
        .map(FileSystemMasterMetricsTest::createUfsStatusWithName)
        .collect(Collectors.toList());
    ufsStatusCache.addChildren(path, statusList);
    assertEquals(4, cacheSizeTotal.getCount());
    assertEquals(3, cacheChildrenSizeTotal.getCount());

    statusList = ImmutableList.of("1", "2", "3", "4")
        .stream()
        .map(FileSystemMasterMetricsTest::createUfsStatusWithName)
        .collect(Collectors.toList());
    ufsStatusCache.addChildren(path, statusList);
    assertEquals(5, cacheSizeTotal.getCount());
    assertEquals(4, cacheChildrenSizeTotal.getCount());

    ufsStatusCache.remove(path);
    assertEquals(0, cacheSizeTotal.getCount());
    assertEquals(0, cacheChildrenSizeTotal.getCount());
    //remove once more
    ufsStatusCache.remove(path);
    assertEquals(0, cacheSizeTotal.getCount());
    assertEquals(0, cacheChildrenSizeTotal.getCount());
  }

  private Object getGauge(String name) {
    return MetricsSystem.METRIC_REGISTRY.getGauges()
        .get(name).getValue();
  }

  private Counter getCounter(String name) {
    return MetricsSystem.METRIC_REGISTRY.getCounters()
        .get(name);
  }

  private static UfsStatus createUfsStatusWithName(String name) {
    return new UfsFileStatus(name, "hash", 0, 0L, "owner", "group", (short) 0, null, 0);
  }
}
