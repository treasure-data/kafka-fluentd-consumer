package org.fluentd.kafka.parser;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import kafka.message.MessageAndMetadata;

import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;
import org.joni.NameEntry;

import org.fluentd.kafka.PropertyConfig;

public class RegexpParser extends MessageParser {
    private final Regex regex;
    private final NamedGroupEntry[] entries;

    public RegexpParser(PropertyConfig config) {
        super(config);

        byte[] pattern = config.get("fluentd.record.pattern").getBytes(StandardCharsets.UTF_8);
        regex = new Regex(pattern, 0, pattern.length, Option.DEFAULT, UTF8Encoding.INSTANCE);
        entries = setupNamedGroupEntries();
    }

    @Override
    public Map<String, Object> parse(MessageAndMetadata<byte[], byte[]> entry) throws Exception {
        HashMap<String, Object> data = new HashMap<String, Object>();
        byte[] rawMessage = entry.message();
        Matcher matcher = regex.matcher(rawMessage);

        int result = matcher.search(0, rawMessage.length, Option.DEFAULT);
        if (result != -1) {
            Region region = matcher.getEagerRegion();
            for (NamedGroupEntry e : entries) {
                int index = e.index;
                if (region.beg[index] == -1)
                    continue;

                String value = new String(rawMessage, region.beg[index], region.end[index] - region.beg[index], StandardCharsets.UTF_8);
                data.put(e.name, value);
            }
        } else {
            throw new RuntimeException("message has wrong format: message = " + new String(rawMessage, StandardCharsets.UTF_8));
        }

        return data;
    }

    private NamedGroupEntry[] setupNamedGroupEntries() {
        NamedGroupEntry[] entries = new NamedGroupEntry[regex.numberOfNames()];
        int i = 0;

        for (Iterator<NameEntry> entryIt = regex.namedBackrefIterator(); entryIt.hasNext();) {
            NameEntry e = entryIt.next();
            int index = e.getBackRefs()[0];

            entries[i] = new NamedGroupEntry(new String(e.name, e.nameP, e.nameEnd - e.nameP, StandardCharsets.UTF_8), index);
            i++;
        }

        return entries;
    }

    private static final class NamedGroupEntry {
        public final String name;
        public final int index;

        public NamedGroupEntry(String name, int index) {
            this.name = name;
            this.index = index;
        }
    }
}
