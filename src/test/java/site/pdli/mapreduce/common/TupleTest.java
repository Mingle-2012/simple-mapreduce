package site.pdli.mapreduce.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TupleTest {
    @Test
    public void testToBytes() {
        Tuple<String, String> tuple = new Tuple<>("key", "value");
        byte[] bytes = tuple.toBytes();
        assertEquals("\"key\"|\"value\"", new String(bytes));
    }

    @Test
    public void testFromBytes() {
        byte[] bytes = "\"key\"|\"value\"".getBytes();
        Tuple<String, String> tuple = Tuple.fromBytes(bytes);
        assertEquals("key", tuple.key());
        assertEquals("value", tuple.value());
    }

    @Test
    public void testFromBytesWithSpecialCharacters() {
        byte[] bytes = "\"key\"|\"value|with|pipes\"".getBytes();
        Tuple<String, String> tuple = Tuple.fromBytes(bytes);
        assertEquals("key", tuple.key());
        assertEquals("value|with|pipes", tuple.value());
    }
}
