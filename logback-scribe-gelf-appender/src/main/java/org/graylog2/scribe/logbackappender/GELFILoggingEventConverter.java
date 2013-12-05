package org.graylog2.scribe.logbackappender;

import ch.qos.logback.classic.pattern.ExtendedThrowableProxyConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.util.LevelToSyslogSeverity;
import ch.qos.logback.core.net.SyslogConstants;
import org.slf4j.Marker;

import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for formatting a log event into a GELF message
 * <p/>
 * {@see https://github.com/Moocar/logback-gelf/blob/master/src/main/java/me/moocar/logbackgelf/GelfConverter.java}
 *
 * @author James Furness
 * @author Anthony Marcar
 */
public class GELFILoggingEventConverter extends GELFConverter<ILoggingEvent> {
    private final boolean useLoggerName;
    private final boolean useThreadName;
    private final Map<String, String> additionalFields;
    private final ExtendedThrowableProxyConverter converter = new ExtendedThrowableProxyConverter();

    public GELFILoggingEventConverter(String facility,
                                      boolean useLoggerName,
                                      boolean useThreadName,
                                      Map<String, String> additionalFields,
                                      int shortMessageLength,
                                      String hostname,
                                      String processId) {
        super(facility, processId, shortMessageLength, hostname);

        this.useLoggerName = useLoggerName;
        this.useThreadName = useThreadName;
        this.additionalFields = additionalFields;
        converter.start();

    }

    /**
     * Creates a map of properties that represent the GELF message.
     *
     * @param logEvent The log event
     * @return map of gelf properties
     */
    @Override
    protected Map<String, Object> mapFields(ILoggingEvent logEvent) {
        Map<String, Object> map = new HashMap<String, Object>();

        map.put("facility", facility);
        map.put("host", hostname);
        map.put("_pid", processId);

        String message = logEvent.getFormattedMessage();

        // Format up the stack trace
        IThrowableProxy proxy = logEvent.getThrowableProxy();
        if (proxy != null) {
            String fullMessage = message + "\n" + converter.convert(logEvent);
            map.put("full_message", fullMessage);
            map.put("short_message", truncateToShortMessage(message + ", " + proxy.getClassName() + ": " + proxy.
                    getMessage()));
        } else {
            map.put("full_message", message);
            map.put("short_message", truncateToShortMessage(message));
        }

        // Ever since version 0.9.6, GELF accepts timestamps in decimal form.
        double logEventTimeTimeStamp = logEvent.getTimeStamp() / 1000.0;

        map.put("timestamp", logEventTimeTimeStamp);
        map.put("version", "1.0");
        map.put("level", LevelToSyslogSeverity.convert(logEvent));

        // If the event was raised by LoggingErrorListener with the alert flag set, bump the severity up to ALERT
        Marker eventsMarker = logEvent.getMarker();
        if (eventsMarker != null) {
            if (eventsMarker.contains("ALERT")) {
                map.put("level", SyslogConstants.ALERT_SEVERITY);
            }
        }

        additionalFields(map, logEvent);

        return map;
    }

    /**
     * Converts the additional fields into proper GELF JSON
     *
     * @param map         The map of additional fields
     * @param eventObject The Logging event that we are converting to GELF
     */
    private void additionalFields(Map<String, Object> map, ILoggingEvent eventObject) {
        if (useLoggerName) {
            map.put("_logger", eventObject.getLoggerName());
        }

        if (useThreadName) {
            map.put("_thread", eventObject.getThreadName());
        }

        Map<String, String> mdc = eventObject.getMDCPropertyMap();

        if (mdc != null) {
            for (String key : additionalFields.keySet()) {
                String field = mdc.get(key);
                if (field != null) {
                    map.put(additionalFields.get(key), field);
                }
            }
        }
    }
}