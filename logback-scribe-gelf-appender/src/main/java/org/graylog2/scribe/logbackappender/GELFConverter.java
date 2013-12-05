package org.graylog2.scribe.logbackappender;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Map;

/**
 * @author Lee Butts
 */
public abstract class GELFConverter<E> {
    protected final String facility;
    protected final int shortMessageLength;
    protected final String hostname;
    protected final String processId;
    protected final Gson gson;

    public GELFConverter(String facility, String processId, int shortMessageLength, String hostname) {
        this.facility = facility;
        this.processId = processId;
        this.shortMessageLength = shortMessageLength;
        this.hostname = hostname;

        // Init GSON for underscores
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);
        this.gson = gsonBuilder.create();
    }

    /**
     * Converts a log event into GELF JSON.
     *
     * @param logEvent The log event we're converting
     * @return The log event converted into GELF JSON
     */
    public String toGelf(E logEvent) {
        try {
            return gson.toJson(mapFields(logEvent));
        } catch (RuntimeException e) {
            throw new IllegalStateException("Error creating JSON message", e);
        }
    }

    protected abstract Map<String, Object> mapFields(E logEvent);

    protected String truncateToShortMessage(String fullMessage) {
        int newLine = fullMessage.indexOf("\n");
        int shortLength = Math.min(shortMessageLength, newLine > 0 ? newLine : Integer.MAX_VALUE);

        if (fullMessage.length() > shortLength) {
            return fullMessage.substring(0, shortLength);
        }

        return fullMessage;
    }
}
