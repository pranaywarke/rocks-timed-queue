package dev.rocksqueue.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class BinaryKeyEncoderTest {

    @Test
    void lexicographicOrderMatchesNumeric() {
        long t1 = 1000L;
        long t2 = 2000L;
        long s1 = 1L;
        long s2 = 2L;

        byte[] k11 = BinaryKeyEncoder.encode(t1, s1);
        byte[] k12 = BinaryKeyEncoder.encode(t1, s2);
        byte[] k21 = BinaryKeyEncoder.encode(t2, s1);

        // t1 < t2 => k11 < k21
        assertTrue(compare(k11, k21) < 0);
        // same timestamp, s1 < s2 => k11 < k12
        assertTrue(compare(k11, k12) < 0);
        // t1 < t2 even if sequence larger
        assertTrue(compare(k12, k21) < 0);

        assertEquals(t1, BinaryKeyEncoder.decodeTimestamp(k11));
        assertEquals(s1, BinaryKeyEncoder.decodeSequence(k11));
    }

    private static int compare(byte[] a, byte[] b) {
        for (int i = 0; i < Math.min(a.length, b.length); i++) {
            int ai = a[i] & 0xFF;
            int bi = b[i] & 0xFF;
            if (ai != bi) return ai - bi;
        }
        return a.length - b.length;
    }
}
