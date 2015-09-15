package org.fluentd.kafka;

public class FluentdTagger {
    private final boolean fixedTag;
    private final String baseTag;

    public FluentdTagger(String tag, String tagPrefix) {
        if (tag != null) {
            fixedTag = true;
            baseTag = tag;
        } else {
            fixedTag = false;
            baseTag = tagPrefix;
        }
    }

    public String generate(String topic) {
        return fixedTag ? baseTag : baseTag + topic;
    }
}
