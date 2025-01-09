package site.pdli.common;

public record Tuple<K, V>(K key, V value) {
    public byte[] toBytes() {
        return toFileString().getBytes();
    }

    public byte[] toBytes(String end) {
        return (toFileString() + end).getBytes();
    }

    public static Tuple<String, String> fromBytes(byte[] bytes) {
        String[] parts = new String(bytes).split("\"\\|\"");
        return new Tuple<>(parts[0].substring(1), parts[1].substring(0, parts[1].length() - 1));
    }

    public String toFileString() {
        return "\"" + key.toString() + "\"" + "|" + "\"" + value.toString() + "\"";
    }
}
