package org.graylog2.scribe.inputplugin;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.thrift.TException;
import org.drools.core.util.ArrayUtils;
import org.graylog2.inputs.gelf.gelf.GELFParser;
import org.graylog2.plugin.InputHost;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.buffers.BatchBuffer;
import org.graylog2.plugin.buffers.BufferOutOfCapacityException;
import org.graylog2.plugin.buffers.ProcessingDisabledException;
import org.graylog2.plugin.inputs.MessageInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scribe.thrift.LogEntry;
import scribe.thrift.ResultCode;
import scribe.thrift.scribe;

import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author James Furness
 * @author @wirehead
 */
public class ScribeGELFProcessor extends GELFParser implements scribe.Iface {
    private static final Logger LOG = LoggerFactory.getLogger(ScribeGELFInput.class);

    private final MessageInput messageInput;
    private final BatchBuffer processBuffer;
    private final Meter incomingMessages;
    private final Meter incompleteMessages;
    private final Meter deferredMessages;
    private final Meter processedMessages;

    public ScribeGELFProcessor(InputHost server, MessageInput messageInput) {
        super(server);
        this.messageInput = messageInput;
        this.processBuffer = server.getProcessBuffer();

        MetricRegistry metrics = server.metrics();
        String metricName = messageInput.getUniqueReadableId();
        this.incomingMessages = metrics.meter(name(metricName, "incomingMessages"));
        this.incompleteMessages = metrics.meter(name(metricName, "incompleteMessages"));
        this.deferredMessages = metrics.meter(name(metricName, "deferredMessages"));
        this.processedMessages = metrics.meter(name(metricName, "processedMessages"));
    }

    @Override
    public ResultCode Log(List<LogEntry> messages) throws TException {
        incomingMessages.mark(messages.size());

        if (processBuffer.getBufferSize() < messages.size()) {
            // Could also try processing the batch in chunks here, but better that the Scribe and downstream publishers are configured correctly to split messages into manageable chunks so the Scribe message can be atomically published to the RingBuffer
            throw new IllegalStateException("Process buffer too small (" + processBuffer.getBufferSize() + ") for bulk insert of " + messages.size() + " messages received from Scribe. Increase process buffer size or decrease Scribe publisher max batch sizes and configured max_size on buffer file storage.");
        }

        if (!processBuffer.hasCapacity(messages.size())) {
            LOG.warn("Process buffer over capacity, returning TRY_LATER. Buffer capacity: " + processBuffer.getUsage() + ", Incoming message count: " + messages.size());
            deferredMessages.mark(messages.size());
            return ResultCode.TRY_LATER;
        }

        Message[] translatedMessages = new Message[messages.size()];
        int i = 0;

        for (LogEntry message : messages) {
            Message lm = parse(message.getMessage(), messageInput);

            if (!lm.isComplete()) {
                incompleteMessages.mark();
                LOG.debug("Skipping incomplete message: " + message);
                continue;
            }

            lm.addField("scribe_category", message.getCategory());

            translatedMessages[i++] = lm;
        }

        if (i < translatedMessages.length) {
            // Resize array due to incomplete messages
            translatedMessages = (Message[]) ArrayUtils.copyOf(translatedMessages, i, Message.class);
        }

        try {
            processBuffer.insertFailFast(translatedMessages, messageInput);
            processedMessages.mark(translatedMessages.length);
            return ResultCode.OK;

        } catch (BufferOutOfCapacityException e) {
            LOG.warn("Process buffer over capacity, returning TRY_LATER. Buffer capacity: " + processBuffer.getUsage() + ", Incoming message count: " + messages.size());
            deferredMessages.mark(translatedMessages.length);
            return ResultCode.TRY_LATER;

        } catch (ProcessingDisabledException e) {
            LOG.warn("Processing disabled, returning TRY_LATER. Buffer capacity: " + processBuffer.getUsage() + ", Incoming message count: " + messages.size());
            deferredMessages.mark(translatedMessages.length);
            return ResultCode.TRY_LATER;

        }
    }
}
