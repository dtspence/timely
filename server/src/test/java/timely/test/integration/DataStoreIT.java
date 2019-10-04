package timely.test.integration;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.tserver.compaction.DefaultCompactionStrategy;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import timely.Server;
import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.api.response.timeseries.QueryResponse;
import timely.configuration.Configuration;
import timely.store.MetricAgeOffIterator;
import timely.store.cache.DataStoreCache;
import timely.store.compaction.MetricCompactionStrategy;
import timely.store.compaction.TieredCompactionStrategy;
import timely.test.IntegrationTest;

@Category(IntegrationTest.class)
public class DataStoreIT extends OneWaySSLBase {

    private static final long ONE_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long ONE_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long TEST_TIME = System.currentTimeMillis() - ONE_DAY;

    @Test
    public void testDefaultAgeOffWithoutCache() throws Exception {
        conf.getCache().setEnabled(false);
        testDefaultAgeOff(conf);
    }

    @Test
    public void testDefaultAgeOffWithCache() throws Exception {
        conf.getCache().setEnabled(true);
        testDefaultAgeOff(conf);
    }

    @Test
    public void testDefaultAgeOffWithPartialCache() throws Exception {
        conf.getCache().setEnabled(true);
        // This test has three values in relation to TEST_TIME = 24 hours ago
        // TEST_TIME, TEST_TIME + 1 hour, TEST_TIME + 2 hours
        // TEST_TIME should be aged off
        // TEST_TIME + 1 should be retrieved from Accumulo
        // TEST_TIME + 2 should be retrieved from the cache
        HashMap<String, Integer> ageOffHours = new HashMap<>();
        ageOffHours.put(DataStoreCache.DEFAULT_AGEOFF_KEY, 23);
        conf.getCache().setMetricAgeOffHours(ageOffHours);
        testDefaultAgeOff(conf);
    }

    @Test
    public void testDefaultAgeOffWithMetricCompactionEnabled() throws Exception {
        conf.getMetricsCompaction().setAutoConfigure(true);
        testDefaultAgeOff(conf);
    }

    public void testDefaultAgeOff(Configuration conf) throws Exception {
        HashMap<String, Integer> ageOffSettings = new HashMap<>();
        ageOffSettings.put(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, 1);
        conf.setMetricAgeOffDays(ageOffSettings);

        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
                    "sys.cpu.user " + (TEST_TIME + ONE_HOUR) + " 3.0 tag1=value1 tag2=value2",
                    "sys.cpu.user " + (TEST_TIME + (ONE_HOUR * 2)) + " 2.0 tag1=value1 tag3=value3");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(4, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + (24 * ONE_HOUR));
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.user");
            subQuery.addTag("tag1", ".*");
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);

            assertEquals(1, response.size());
            Map<String, String> tags = response.get(0).getTags();
            assertEquals(1, tags.size());
            assertEquals("value1", tags.get("tag1"));
            Map<String, Object> dps = response.get(0).getDps();
            assertEquals(2, dps.size());
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testMultipleAgeOffWithoutCache() throws Exception {
        conf.getCache().setEnabled(false);
        testMultipleAgeOff(conf);
    }

    @Test
    public void testMultipleAgeOffWithCache() throws Exception {
        conf.getCache().setEnabled(true);
        testMultipleAgeOff(conf);
    }

    @Test
    public void testMultipleAgeOffWithMetricCompaction() throws Exception {
        conf.getMetricsCompaction().setAutoConfigure(true);
        testMultipleAgeOff(conf);
    }

    public void testMultipleAgeOff(Configuration conf) throws Exception {
        HashMap<String, Integer> ageOffSettings = new HashMap<>();
        ageOffSettings.put(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, 1);
        ageOffSettings.put("sys.cpu.user", 1);
        conf.setMetricAgeOffDays(ageOffSettings);

        final Server s = new Server(conf);
        s.run();
        // @formatter:off
        /*
         * TEST_TIME = System.currentTimeMillis() - ONE_DAY
         * Age off for all metrics is one day = 24 hours
         *
         * lines 1, 2, 3 will age off immediately as they are >= 2 days old
         * line 4 is there because otherwise the meta tags would also age off and we would get a 400 - No Tags Found
         * line 5 is one day old and will age off immediately
         * lines 6 & 7 should be returned as they are not aged off and are within the query range
         *
         */
        // @formatter:on
        try {
            put("sys.cpu.idle " + (TEST_TIME - ONE_DAY - (2 * ONE_HOUR)) + " 1.0 tag1=value1 tag2=value2",
                    "sys.cpu.idle " + (TEST_TIME - ONE_DAY - ONE_HOUR) + " 3.0 tag1=value1 tag2=value2",
                    "sys.cpu.idle " + (TEST_TIME - ONE_DAY) + " 2.0 tag1=value1 tag3=value3",
                    "sys.cpu.idle " + (TEST_TIME + (ONE_DAY * 2)) + " 2.0 tag1=value1 tag3=value3",
                    "sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
                    "sys.cpu.user " + (TEST_TIME + ONE_HOUR) + " 3.0 tag1=value1 tag2=value2",
                    "sys.cpu.user " + (TEST_TIME + (ONE_HOUR * 2)) + " 2.0 tag1=value1 tag3=value3");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(4, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME - ONE_DAY - (2 * ONE_HOUR));
            request.setEnd(TEST_TIME + (24 * ONE_HOUR));
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.user");
            subQuery.addTag("tag1", ".*");
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
            assertEquals(1, response.size());
            Map<String, String> tags = response.get(0).getTags();
            assertEquals(1, tags.size());
            assertEquals("value1", tags.get("tag1"));
            Map<String, Object> dps = response.get(0).getDps();
            assertEquals(2, dps.size());

            QueryRequest request2 = new QueryRequest();
            request2.setStart(TEST_TIME - ONE_DAY - (2 * ONE_HOUR));
            request2.setEnd(TEST_TIME + (24 * ONE_HOUR));
            SubQuery subQuery2 = new SubQuery();
            subQuery2.setMetric("sys.cpu.idle");
            subQuery2.addTag("tag1", ".*");
            subQuery2.setDownsample(Optional.of("1s-max"));
            request2.addQuery(subQuery2);
            List<QueryResponse> response2 = query("https://127.0.0.1:54322/api/query", request2);
            assertEquals(0, response2.size());

        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testMetricCompactionPropertyDisabledByDefault() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            Connector con = mac.getConnector(MAC_ROOT_USER, MAC_ROOT_PASSWORD);
            Map<String, String> checkOpts = ImmutableMap
                    .copyOf(con.tableOperations().getProperties(conf.getMetricsTable()));
            String class1 = Property.TABLE_COMPACTION_STRATEGY_PREFIX + "tier.0.class";
            assertFalse(checkOpts.containsKey(class1));
            assertEquals(DefaultCompactionStrategy.class.getName(),
                    checkOpts.get(Property.TABLE_COMPACTION_STRATEGY.getKey()));
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testMetricCompactionRemovesExistingKeys() throws Exception {
        String testKey = Property.TABLE_COMPACTION_STRATEGY_PREFIX + "tier.n.class";
        conf.getMetricsCompaction().setAutoConfigure(true);
        Connector con = mac.getConnector(MAC_ROOT_USER, MAC_ROOT_PASSWORD);
        if (!con.namespaceOperations().exists("timely")) {
            con.namespaceOperations().create("timely");
        }
        con.tableOperations().create(conf.getMetricsTable());
        con.tableOperations().setProperty(conf.getMetricsTable(),
                Property.TABLE_COMPACTION_STRATEGY_PREFIX + "tier.n.class", "test-value");
        final Server s = new Server(conf);
        s.run();
        try {
            Map<String, String> checkOpts = ImmutableMap
                    .copyOf(con.tableOperations().getProperties(conf.getMetricsTable()));
            assertFalse(checkOpts.containsKey(testKey));
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testMetricCompactionPropertyCreation() throws Exception {
        conf.getMetricsCompaction().setAutoConfigure(true);
        conf.getMetricsCompaction().getDefaultStrategyOptions().put("test1", "value1");
        conf.getMetricsCompaction().getMetricStrategyOptions().put("test2", "value2");
        final Server s = new Server(conf);
        s.run();
        try {
            String class1 = Property.TABLE_COMPACTION_STRATEGY_PREFIX + "tier.0.class";
            String class2 = Property.TABLE_COMPACTION_STRATEGY_PREFIX + "tier.1.class";
            String optKey1 = Property.TABLE_COMPACTION_STRATEGY_PREFIX + "tier.0.opts.test1";
            String optKey2 = Property.TABLE_COMPACTION_STRATEGY_PREFIX + "tier.1.opts.test2";
            Connector con = mac.getConnector(MAC_ROOT_USER, MAC_ROOT_PASSWORD);
            Map<String, String> checkOpts = ImmutableMap
                    .copyOf(con.tableOperations().getProperties(conf.getMetricsTable()));
            assertTrue(checkOpts.containsKey(class1));
            assertTrue(checkOpts.containsKey(class2));
            assertTrue(checkOpts.containsKey(optKey1));
            assertTrue(checkOpts.containsKey(optKey2));
            assertEquals(DefaultCompactionStrategy.class.getName(), checkOpts.get(class1));
            assertEquals(MetricCompactionStrategy.class.getName(), checkOpts.get(class2));
            assertEquals("value1", checkOpts.get(optKey1));
            assertEquals("value2", checkOpts.get(optKey2));
            assertEquals(TieredCompactionStrategy.class.getName(),
                    checkOpts.get(Property.TABLE_COMPACTION_STRATEGY.getKey()));
        } finally {
            s.shutdown();
        }
    }
}
