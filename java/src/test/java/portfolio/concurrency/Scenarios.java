package portfolio.concurrency;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Reads spec/scenarios.json, the parameters shared by all three languages. */
final class Scenarios {
    private static final Map<String, Object> ROOT = load();

    private Scenarios() {
    }

    /** Integer at a dotted path such as "pipeline.workers.square". */
    static int get(String path) {
        Object cur = ROOT;
        for (String part : path.split("\\.")) {
            cur = ((Map<?, ?>) cur).get(part);
            if (cur == null) {
                throw new IllegalArgumentException("no scenario value " + path);
            }
        }
        return ((Number) cur).intValue();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> load() {
        try {
            return (Map<String, Object>) new MiniJson(Files.readString(Path.of("../spec/scenarios.json"))).value();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Just enough JSON for the scenarios file: objects, strings and integers. */
    private static final class MiniJson {
        private final String s;
        private int i;

        MiniJson(String s) {
            this.s = s;
        }

        Object value() {
            skip();
            char c = s.charAt(i);
            if (c == '{') {
                i++;
                Map<String, Object> m = new LinkedHashMap<>();
                skip();
                if (s.charAt(i) == '}') {
                    i++;
                    return m;
                }
                while (true) {
                    skip();
                    String key = (String) value();
                    skip();
                    expect(':');
                    m.put(key, value());
                    skip();
                    if (s.charAt(i) == ',') {
                        i++;
                    } else {
                        expect('}');
                        return m;
                    }
                }
            }
            if (c == '"') {
                int end = s.indexOf('"', i + 1);
                String str = s.substring(i + 1, end);
                i = end + 1;
                return str;
            }
            int start = i;
            while (i < s.length() && (Character.isDigit(s.charAt(i)) || s.charAt(i) == '-')) {
                i++;
            }
            return Long.parseLong(s.substring(start, i));
        }

        private void skip() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }

        private void expect(char c) {
            if (s.charAt(i) != c) {
                throw new IllegalStateException("expected " + c + " at " + i);
            }
            i++;
        }
    }
}
