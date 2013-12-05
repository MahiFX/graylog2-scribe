package org.graylog2.scribe.logbackappender;

import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.DeferredProcessingAware;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import scribe.thrift.LogEntry;
import scribe.thrift.ResultCode;
import scribe.thrift.scribe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Simple GELF Appender. <b>Note this is blocking and WILL block your application threads whilst waiting for Scribe</b>.
 * TODO: Publish non-blocking version!
 *
 * @author James Furness
 */
public class ScribeBlockingGELFAppender<T extends DeferredProcessingAware> extends UnsynchronizedAppenderBase<T> {
    private static final int SOCKET_TIMEOUT = Integer.getInteger("ScribeBlockingGELFAppender.SOCKET_TIMEOUT", 10000);
    private static final long MIN_BACKOFF_SLEEP = Long.getLong("ScribeBlockingGELFAppender.MIN_BACKOFF_SLEEP", 100L);
    private static final long MAX_BACKOFF_SLEEP = Long.getLong("ScribeBlockingGELFAppender.MAX_BACKOFF_SLEEP", 30000);
    private static final int MAX_RETRIES = Integer.getInteger("ScribeBlockingGELFAppender.MAX_RETRIES", 50); // ~20 minutes

    private final String scribeHost;
    private final int scribePort;
    private final String scribeCategory;

    private final ArrayList<LogEntry> logEntries = new ArrayList<LogEntry>();
    private final GELFConverter<T> converter;

    private scribe.Client client;
    private TFramedTransport transport;

    public ScribeBlockingGELFAppender(String scribeHost, int scribePort, String scribeCategory, GELFConverter<T> gelfConverter) {
        this.scribeHost = scribeHost;
        this.scribePort = scribePort;
        this.scribeCategory = scribeCategory;
        this.converter = gelfConverter;
    }

    @Override
    public synchronized void start() {
        addInfo("ScribeBlockingGELFAppender starting, sending logs to " + scribeHost + ":" + scribePort);

        TSocket sock = new TSocket(scribeHost, scribePort, SOCKET_TIMEOUT);
        transport = new TFramedTransport(sock);

        try {
            transport.open();
            addInfo("TSocket connected to " + scribeHost + ":" + scribePort);

        } catch (Exception e) {
            addWarn("Failed to connect to " + scribeHost + ":" + scribePort, e);

        }

        TBinaryProtocol protocol = new TBinaryProtocol(transport, false, false);
        client = new scribe.Client(protocol, protocol);

        super.start();
    }

    @Override
    public synchronized void stop() {
        super.stop();

        if (transport != null && transport.isOpen()) {
            transport.close();
        }
    }

    @Override
    protected void append(T eventObject) {
        processBuffer(Collections.singletonList(eventObject));
    }

    protected void processBuffer(List<T> events) {
        try {
            logEntries.ensureCapacity(events.size());

            for (Object event : events) {
                logEntries.add(new LogEntry(scribeCategory, converter.toGelf((T) event)));
            }

            long sleepPeriod = MIN_BACKOFF_SLEEP;
            Exception lastException = null;

            for (int i = 0; i < MAX_RETRIES; i++) {
                try {
                    if (!transport.isOpen()) {
                        transport.close();
                        transport.open();
                        addInfo("TSocket connected to " + scribeHost + ":" + scribePort);
                    }

                    ResultCode result = client.Log(logEntries);

                    if (ResultCode.OK.equals(result)) {
                        return;
                    }

                    addWarn("Received " + result + ", retrying in " + sleepPeriod + "ms");

                } catch (Exception e) {
                    lastException = e;
                    addWarn("Failed to log events, closing transport and retrying in " + sleepPeriod + "ms", e);
                    transport.close();

                }

                Thread.sleep(sleepPeriod);
                sleepPeriod = Math.min(sleepPeriod * 2, MAX_BACKOFF_SLEEP);
            }

            throw new IllegalStateException("Failed to send events to Scribe after " + MAX_RETRIES + " attempts", lastException);

        } catch (Exception e) {
            addError("Failed to log " + logEntries.size() + " events", e);

            for (LogEntry entry : logEntries) {
                addWarn("FAIL: " + entry);
            }

        } finally {
            logEntries.clear();

        }
    }
}
