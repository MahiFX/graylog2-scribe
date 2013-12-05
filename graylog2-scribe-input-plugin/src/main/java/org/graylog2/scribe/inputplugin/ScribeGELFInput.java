package org.graylog2.scribe.inputplugin;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.*;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationException;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scribe.thrift.scribe;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * @author James Furness
 * @author @wirehead
 */
public class ScribeGELFInput extends MessageInput {
    private static final Logger LOG = LoggerFactory.getLogger(ScribeGELFInput.class);

    public static final String NAME = "Scribe GELF Input";

    private TNonblockingServerTransport socket;
    private TServer server;

    @Override
    public void checkConfiguration() throws ConfigurationException {
        if (!checkConfig(configuration)) {
            throw new ConfigurationException(configuration.getSource().toString());
        }
    }

    public static final String HOST = "scribe_host";
    public static final String PORT = "scribe_port";
    public static final String MAX_LENGTH = "scribe_max_message_length";
    public static final String WORKER_THREADS = "scribe_worker_threads";

    @Override
    public void launch() throws MisfireException {
        String host = configuration.getString(HOST);
        int port = (int) configuration.getInt(PORT);
        int thrift_length = (int) configuration.getInt(MAX_LENGTH);
        int worker_threads = (int) configuration.getInt(WORKER_THREADS);

        LOG.info("Starting Scribe server on port: " + port);

        scribe.Processor<ScribeGELFProcessor> processor = new scribe.Processor<ScribeGELFProcessor>(
                new ScribeGELFProcessor(
                        graylogServer,
                        this
                )
        );

        try {
            socket = new TNonblockingServerSocket(new InetSocketAddress(host, port));

        } catch (TTransportException e) {
            throw new RuntimeException(String.format("Unable to create scribe server socket to %s:%s", host, port), e);

        }

        // Protocol factory
        TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(
                false,
                false,
                thrift_length
        );

        // Transport factory
        TTransportFactory inTransportFactory, outTransportFactory;
        inTransportFactory = new TFramedTransport.Factory(thrift_length);
        outTransportFactory = new TFramedTransport.Factory(thrift_length);
        LOG.info("Using TFastFramedTransport with a max frame size of " + String.valueOf(thrift_length) + " bytes");

        // ThreadPool Server
        THsHaServer.Args args = new THsHaServer.Args(socket)
                .inputTransportFactory(inTransportFactory)
                .outputTransportFactory(outTransportFactory)
                .inputProtocolFactory(tProtocolFactory)
                .outputProtocolFactory(tProtocolFactory)
                .processor(processor)
                .workerThreads(worker_threads);

        server = new THsHaServer(args);
        server.serve();
    }

    @Override
    public void stop() {
        if (socket != null) {
            socket.close();
        }

        if (server != null) {
            server.stop();
        }
    }

    @Override
    public ConfigurationRequest getRequestedConfiguration() {
        ConfigurationRequest cr = new ConfigurationRequest();

        cr.addField(new TextField(
                HOST,
                "Scribe server bind host",
                "127.0.0.1",
                "Host to bind to in order to listen for inbound Scribe connections",
                ConfigurationField.Optional.OPTIONAL
        ));

        cr.addField(new TextField(
                PORT,
                "Scribe server listen port",
                "1464",
                "Port to listen on for inbound Scribe connections",
                ConfigurationField.Optional.OPTIONAL

        ));

        cr.addField(new NumberField(
                MAX_LENGTH,
                "Scribe max message length",
                16384000,
                "Maximum length of an inbound message (TFramedTransport)",
                ConfigurationField.Optional.OPTIONAL)
        );

        cr.addField(new NumberField(
                WORKER_THREADS,
                "Worker threadpool size",
                5,
                "Size of THsHaServer NIO worker threadpool to receive inbound messages.",
                ConfigurationField.Optional.OPTIONAL)
        );

        return cr;
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String linkToDocs() {
        return "";
    }

    @Override
    public Map<String, Object> getAttributes() {
        return configuration.getSource();
    }

    protected boolean checkConfig(Configuration config) {
        return config.stringIsSet(HOST)
                && config.intIsSet(PORT) && config.getInt(PORT) > 0
                && config.intIsSet(MAX_LENGTH) && config.getInt(MAX_LENGTH) > 0
                && config.intIsSet(WORKER_THREADS) && config.getInt(WORKER_THREADS) > 0;
    }
}
