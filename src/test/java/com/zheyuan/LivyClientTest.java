package com.zheyuan;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import okhttp3.Response;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Please create a local Livy Session for Unit testing.
 * For more information, please read:
 * https://livy.incubator.apache.org/
 */
public class LivyClientTest {
    public LivyClientTest() throws IOException {
    }

    private final String host = "http://slc09xqm.us.oracle.com:8998";
    LivyClient client = new LivyClient(host);

    @Test
//    @Ignore
    public void testGetSessions() throws IOException {
        Response r = client.getSessionIds();
        Set<Integer> sessionIds = LivyClient.sessionIds;
        for (int id : sessionIds) {
            System.out.println("Found live sessions: " + id);
        }
        assert (r.isSuccessful());
    }

    @Test
    @Ignore
    public void testCreateSession() throws IOException {
        Response r = client.createSession();
        assert (r.isSuccessful());
    }

    @Test
    @Ignore
    public void testDeleteSessions() throws IOException {
        Set<Integer> sessionIds = LivyClient.sessionIds;
        Response r = client.deleteSession(6);
//        Response r = client.deleteSession(sessionIds.iterator().next());
        assert (r.isSuccessful());
    }

    @Test
    @Ignore
    public void testCreateStatementWithQuery() throws IOException {
        final String query = "spark.range(1, 20).show(false)";
        Response r = client.createStatementWithQuery(query);
        assert (r.isSuccessful());
    }

    @Test
    @Ignore
    public void testCreateStatementWithFile() throws IOException {
        final Path p = Paths.get("query1.scala");
        Response r = client.createStatementWithFile(p);
        assert (r.isSuccessful());
    }

    @Test
//    @Ignore
    public void testStressTest1() throws IOException, InterruptedException {
        final Path p = Paths.get("query1.scala");
        int i = 20;
        do {
            client.createStatementWithFile(p);
            Thread.sleep(1000L);
        } while (--i > 0);
    }

    @Test
    @Ignore
    public void testStressTest2() throws IOException, InterruptedException {
        final String query =
            "spark.conf.set(\"spark.sql.crossJoin.enabled\", \"true\");" +
            "spark.range(1, 40).join(spark.range(1,30)).show(false)";
        int i = 20;
        do {
            client.createStatementWithQuery(query);
            Thread.sleep(1000L);
        } while (--i > 0);
    }
}
