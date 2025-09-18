package dev.rocksqueue.integration;

import dev.rocksqueue.api.TimeQueue;
import dev.rocksqueue.client.QueueClient;
import dev.rocksqueue.config.CacheType;
import dev.rocksqueue.config.QueueConfig;
import dev.rocksqueue.ser.Utf8StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class QueueThroughputSmokeTest {

    private QueueClient client;
    private Path tmp;

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) client.close();
        if (tmp != null) {
            try {
                Files.walk(tmp)
                        .sorted((a,b) -> b.getNameCount() - a.getNameCount())
                        .forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignored) {} });
            } catch (Exception ignored) {}
        }
    }

    private static int getIntProp(String key, int def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return def; }
    }
    private static boolean getBoolProp(String key, boolean def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        return Boolean.parseBoolean(v);
    }
    private static int getBatchProp(String key, int def) {
        String v = System.getProperty(key);
        if (v == null) return def;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return def; }
    }

    private static String makeAsciiPayload(int bytes) {
        if (bytes <= 0) return "";
        return "a".repeat(bytes);
    }

    @Test
    void enqueue_100k_and_dequeue_immediately_print_throughput() throws Exception {
        int n = getIntProp("q.n", 1000000);
        boolean disableWAL = getBoolProp("q.disableWAL", false);
        boolean syncWrites = getBoolProp("q.syncWrites", false);
        int batch = getBatchProp("q.batch", 20000);
        int payloadBytes = getIntProp("q.payloadBytes", 5024);

        String payload = makeAsciiPayload(payloadBytes);

        tmp = Files.createTempDirectory("rocksqueue-throughput-");
        QueueConfig cfg = new QueueConfig()
                .setBasePath(tmp.toString())
                .setDisableWAL(disableWAL)
                .setSyncWrites(syncWrites)
                .setCacheType(CacheType.IN_MEMORY)
                .   setCacheFileSizeMB(1024)
                .setCacheForceOnWrite(false)
                .setDequeueBatchSize(batch);
        client = new QueueClient(cfg);

        client.registerQueue("tp", String.class, new Utf8StringSerializer());
        TimeQueue<String> q = client.getQueue("tp");

        long now = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            q.enqueue(payload, now);
        }

        long start = System.nanoTime();
        long lastChunkStart = start;
        int lastChunkGot = 0;
        final int chunk = 100_000;
        int got = 0;
        String v;
        while (got < n) {
            v = q.dequeue();
            if (v == null) { Thread.yield(); continue; }
            got++;
            if (got % chunk == 0) {
                long nowNanos = System.nanoTime();
                double chunkSec = (nowNanos - lastChunkStart) / 1_000_000_000.0;
                double chunkTps = chunk / Math.max(1e-6, chunkSec);
                double totalSec = (nowNanos - start) / 1_000_000_000.0;
                double totalTps = got / Math.max(1e-6, totalSec);
//                System.out.println("[progress] got=" + got +
//                        " chunk=" + chunk +
//                        " chunkThroughput=" + String.format("%.2f", chunkTps) + "/s" +
//                        " totalThroughput=" + String.format("%.2f", totalTps) + "/s" +
//                        " elapsed=" + String.format("%.2f", totalSec) + "s");
                lastChunkStart = nowNanos;
                lastChunkGot = got;
            }
        }
        long end = System.nanoTime();

        double sec = (end - start) / 1_000_000_000.0;
        double tps = n / Math.max(1e-6, sec);
        System.out.println("QueueThroughputSmokeTest: n=" + n +
                " disableWAL=" + disableWAL + " syncWrites=" + syncWrites +
                " batch=" + batch +
                " dequeueThroughput=" + String.format("%.2f", tps) + " items/sec in " + String.format("%.2f", sec) + "s");

        assertEquals(n, got);
    }
}
