package dev.rocksqueue.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class UtilsSanitizeTest {

    @Test
    void replacesEverySeparatorAndUnsafeCharacter() {
        assertEquals("orders_2024", Utils.sanitize("orders/2024"));
        assertEquals("orders_2024", Utils.sanitize("orders\\2024"));
        assertEquals("a_b_c", Utils.sanitize("a b c"));
        assertEquals("keeps.the-safe_ones", Utils.sanitize("keeps.the-safe_ones"));
    }

    @Test
    void rejectsNamesThatResolveToAPathTraversal() {
        assertThrows(IllegalArgumentException.class, () -> Utils.sanitize(".."));
        assertThrows(IllegalArgumentException.class, () -> Utils.sanitize("."));
        // Separators are replaced before the check, so only a name that is exactly
        // "." or ".." is a traversal; "/../" is already reduced to a harmless "_.._".
        assertEquals("_.._", Utils.sanitize("/../"));
    }

    @Test
    void keepsNamesThatMerelyLookLikeDots() {
        // Only "." and ".." are resolved by the filesystem. "..." and ".hidden" are
        // ordinary filenames and must survive, or valid group names would be refused.
        assertEquals("...", Utils.sanitize("..."));
        assertEquals(".hidden", Utils.sanitize(".hidden"));
        assertEquals("..a", Utils.sanitize("..a"));
    }

    @Test
    void rejectsBlankNames() {
        assertThrows(IllegalArgumentException.class, () -> Utils.sanitize(null));
        assertThrows(IllegalArgumentException.class, () -> Utils.sanitize(""));
        assertThrows(IllegalArgumentException.class, () -> Utils.sanitize("   "));
    }
}
