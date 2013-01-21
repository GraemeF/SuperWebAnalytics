package swa;

import backtype.cascading.tap.PailTap;
import backtype.hadoop.pail.Pail;
import backtype.hadoop.pail.PailSpec;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascalog.ops.IdentityBuffer;
import cascalog.ops.RandLong;
import com.google.common.collect.Maps;
import jcascalog.Api;
import jcascalog.Fields;
import jcascalog.Option;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.Sum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import swa.cascalog.*;
import swa.generated.DataUnit;
import swa.pail.SplitDataPailStructureOld;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Main {
    public static final String NEW_DATA_LOCATION = "/tmp/swa/newData";
    public static final String MASTER_DATA_LOCATION = "/tmp/swa/masterData";
    public static final String SHREDDED_DATA_LOCATION = "/tmp/swa/shredded";
    public static final String UNIQUE_PAGEVIEW_LOCATION = "/tmp/swa/unique_pageviews";
    public static final String NORMALIZED_PAGEVIEW_LOCATION = "/tmp/swa/normalized_pageview_users";
    public static final String NORMALIZED_URLS_LOCATION = "/tmp/swa/normalized_urls";
    public static final String SNAPSHOT_LOCATION = "/tmp/swa/newDataSnapshot";
    public static final String TEMP_DIR = "/tmp/swa";

    public static void main(String[] args) throws Exception {
        setUp();

        Pail newDataPail = new Pail(NEW_DATA_LOCATION);
        Pail masterPail = new Pail(MASTER_DATA_LOCATION);

        // Snapshot the new data swa.pail
        Pail snapshotPail = newDataPail.snapshot(SNAPSHOT_LOCATION);

        // Shred the new data into a shredded data swa.pail
        shredNewData(SNAPSHOT_LOCATION, SHREDDED_DATA_LOCATION);

        // Consolidate the shredded swa.pail then absorb it into the master data
        consolidateAndAbsorbShreddedPail(masterPail, new Pail(SHREDDED_DATA_LOCATION));

        // Delete the snapshot data from the new data pail
        newDataPail.deleteSnapshot(snapshotPail);

        // Normalize the URL's
        normalizeUrls(MASTER_DATA_LOCATION, NORMALIZED_URLS_LOCATION);

        // Normalize the user id's
        normalizeUserIds();

        // Deduplicate page views
        dedupePageViews();

        // Page view rollups
        pageViewRollups();

        // Compute number of bounces
        computeNumberOfBounces();
    }

    private static void consolidateAndAbsorbShreddedPail(Pail masterPail, Pail shreddedPail) throws IOException {
        shreddedPail.consolidate();
        masterPail.absorb(shreddedPail);
    }

    private static void setUp() throws IOException {
        Map<String, String> conf = Maps.newHashMap();
        String sers = "backtype.hadoop.ThriftSerialization";
        sers += ",";
        sers += "org.apache.hadoop.io.serializer.WritableSerialization";
        conf.put("io.serializations", sers);
        Api.setApplicationConf(conf);

        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(TEMP_DIR), true);
        fs.mkdirs(new Path(TEMP_DIR));
    }

    private static void computeNumberOfBounces() {
        PailTap source = new PailTap(UNIQUE_PAGEVIEW_LOCATION);
        Subquery userVisits =
                new Subquery("?domain", "?user", "?num-user-visits", "?num-user-bounces")
                        .predicate(source, "?pageview")
                        .predicate(new ExtractPageViewFields(), "?pageview").out("?url", "?user", "?timestamp")
                        .predicate(new ExtractDomain(), "?url").out("?domain")
                        .predicate(Option.SORT, "?timestamp")
                        .predicate(new AnalyzeVisits(), "?timestamp").out("?num-user-visits", "?num-user-bounces");

        Subquery visitsAndBounces = new Subquery("?domain", "?num-visits", "?num-bounces")
                .predicate(userVisits, "?domain", "_", "?num-user-visits", "?num-user-bounces")
                .predicate(new Sum(), "?num-user-visits").out("?num-visits")
                .predicate(new Sum(), "?num-user-bounces").out("?num-bounces");
    }

    private static void pageViewRollups() {
        Subquery pageViews = new Subquery("?url", "?granularity", "?bucket", "?total-pageviews")
                .predicate(rollupHourlyQuery(), "?url", "?hour-bucket", "?count")
                .predicate(new EmitGranularities(), "?hour-bucket").out("?granularity", "?bucket")
                .predicate(new Sum(), "?count").out("?total-pageviews");
    }

    private static Subquery rollupHourlyQuery() {
        PailTap source;
        source = new PailTap(UNIQUE_PAGEVIEW_LOCATION);
        return new Subquery("?url", "?hour-bucket", "?count")
                .predicate(source, "?pageview")
                .predicate(new ExtractPageViewFields(), "?pageview").out("?url", "_", "?timestamp")
                .predicate(new ToHourBucket(), "?timestamp").out("?hour-bucket")
                .predicate(new Count(), "?count");
    }

    private static void dedupePageViews() {
        PailTap source;Tap outTap;
        source = attributeTap(NORMALIZED_PAGEVIEW_LOCATION, DataUnit._Fields.PAGE_VIEW);
        outTap = splitDataTap(UNIQUE_PAGEVIEW_LOCATION);
        Api.execute(outTap,
                new Subquery("?data")
                        .predicate(source, "?data")
                        .predicate(Option.DISTINCT, true));
    }

    private static void normalizeUserIds() throws IOException {
        // Transform equiv Data objects into 2-tuples
        Tap equivs = attributeTap(NORMALIZED_URLS_LOCATION,
                DataUnit._Fields.EQUIV);
        Api.execute(Api.hfsSeqfile(TEMP_DIR + "/equivs0"),
                new Subquery("?node1", "?node2")
                        .predicate(equivs, "_", "?data")
                        .predicate(new EdgifyEquiv(), "?node1", "?node2"));

        int i = 1;
        while(true) {
            Tap progressEdgesSink = runUserIdNormalizationIteration(i);
            if(!new HadoopFlowProcess(new JobConf())
                    .openTapForRead(progressEdgesSink)
                    .hasNext()) {
                break;
            }
            i++;
        }

        Tap pageviews = attributeTap(NORMALIZED_URLS_LOCATION, DataUnit._Fields.PAGE_VIEW);
        Tap newIds = (Tap) Api.hfsSeqfile("/tmp/swa/equivs" + i);
        Tap result = splitDataTap(NORMALIZED_PAGEVIEW_LOCATION);
        Api.execute(result,
                new Subquery("?normalized-pageview")
                        .predicate(newIds, "!!newId", "?person")
                        .predicate(pageviews, "_", "?data")
                        .predicate(new ExtractPageViewFields(), "_", "?person", "_")
                        .predicate(new MakeNormalizedPageview(), "!!newId", "?data").out("?normalized-pageview"));
    }

    private static Tap runUserIdNormalizationIteration(int i) {
        Tap source = (Tap) Api.hfsSeqfile("/tmp/swa/equivs" + (i - 1));
        Tap sink = (Tap) Api.hfsSeqfile("/tmp/swa/equivs" + i);
        Subquery iteration = new Subquery("?b1", "?node1", "?node2", "?is-new")
                .predicate(source, "?n1", "?n2")
                .predicate(new BidirectionalEdge(), "?n1", "?n2").out("?b1", "?b2")
                .predicate(new IterateEdges(), "?b2").out("?node1", "?node2", "?is-new");
        iteration = (Subquery) Api.selectFields(iteration, new Fields("?node1", "?node2", "?is-new"));
        Subquery newEdgeSet = new Subquery("?node1", "?node2")
                .predicate(iteration, "?node1", "?node2", "_")
                .predicate(Option.DISTINCT, true);

        Api.execute(sink, newEdgeSet);

        Tap progressEdgesSink = (Tap) Api.hfsSeqfile("/tmp/swa/equivs" + i + "-new");
        Subquery progressEdges = new Subquery("?node1", "?node2")
                .predicate(iteration, "?node1", "?node2", true);

        Api.execute(progressEdgesSink, progressEdges);

        return progressEdgesSink;
    }

    private static void normalizeUrls(String masterDataLocation, String normalizedUrlsLocation) {
        Tap masterDataset = new PailTap(masterDataLocation);
        Tap outTap = splitDataTap(normalizedUrlsLocation);
        Api.execute(outTap,
                new Subquery("?normalized")
                        .predicate(masterDataset, "_", "?raw")
                        .predicate(new NormalizeURL(), "?raw").out("?normalized"));
    }

    private static void shredNewData(String snapshotLocation, String shreddedDataLocation) {
        PailTap source = new PailTap(snapshotLocation);
        PailTap sink = splitDataTap(shreddedDataLocation);
        Subquery reduced = new Subquery("?rand", "?data")
                .predicate(source, "_", "?data-in")
                .predicate(new RandLong(), "?rand")
                .predicate(new IdentityBuffer(), "?data-in").out("?data");

        Api.execute(
                sink,
                new Subquery("?data")
                        .predicate(reduced, "_", "?data"));
    }

    public static PailTap attributeTap(
            String path,
            final DataUnit._Fields... fields) {
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.attrs = new List[] {
                new ArrayList<String>() {{
                    for(DataUnit._Fields field: fields) {
                        add("" + field.getThriftFieldId());
                    }
                }}
        };
        return new PailTap(path, opts);
    }

    public static PailTap splitDataTap(String path) {
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.spec = new PailSpec(new SplitDataPailStructureOld());
        return new PailTap(path, opts);
    }

    public static void uniquesView() {
        Tap source = new PailTap(UNIQUE_PAGEVIEW_LOCATION);
        Subquery hourlyRollup = new Subquery("?url", "?hour-bucket", "?hyper-log-log")
                        .predicate(source, "?pageview")
                        .predicate(new ExtractPageViewFields(), "?pageview").out("?url", "?user", "?timestamp")
                        .predicate(new ToHourBucket(), "?timestamp").out("?hour-bucket")
                        .predicate(new ConstructHyperLogLog(), "?user").out("?hyper-log-log");
        new Subquery("?url", "?granularity", "?bucket", "?aggregate-hll")
                .predicate(hourlyRollup, "?url", "?hour-bucket", "?hourly-hll")
                .predicate(new EmitGranularities(), "?hour-bucket").out("?granularity", "?bucket")
                .predicate(new MergeHyperLogLog(), "?hourly-hll").out("?aggregate-hll");
    }
}
