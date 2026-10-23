package org.phoebus.recceiver;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.phoebus.recceiver.spi.RecSyncProcessorDispatcher;
import org.phoebus.recceiver.spi.RecSyncProcessorRegistry;

@Configuration
@ComponentScan(basePackages = {"org.phoebus.recceiver"})
@PropertySource(value = "classpath:application.properties")
@SpringBootApplication
public class RecceiverApplication implements CommandLineRunner {

    private static final Logger log = Logger.getLogger(RecceiverApplication.class.getName());

    /** Network address the TCP listener binds to (default 0.0.0.0 = all interfaces). */
    @Value("${tcpBindAddress:0.0.0.0}")
    private String tcpBindAddress;

    /** TCP port to listen on (default 0 = OS-assigned ephemeral port). */
    @Value("${tcpPort:0}")
    private int tcpPort;

    /** TCP idle timeout in seconds (default 15 s). A missed pong closes the connection. */
    @Value("${tcptimeout:15.0}")
    private double tcptimeout;

    /** Size in bytes of the TCP read buffer per connection (default 4096, minimum 8). */
    @Value("${readBufferSize:4096}")
    private int readBufferSize;

    /** Destination address for UDP beacon packets (default 255.255.255.255 limited broadcast). */
    @Value("${udpBroadcastAddress:255.255.255.255}")
    private String udpBroadcastAddress;

    /** UDP port RecSync clients listen on for discovery beacons (default 5049). */
    @Value("${udpBroadcastPort:5049}")
    private int udpBroadcastPort;

    /** Seconds between UDP beacon broadcasts (default 15 s). */
    @Value("${announceInterval:15.0}")
    private double announceInterval;

    /** Seconds between automatic transaction commits (default 5 s). 0 = commit on every upload-complete. */
    @Value("${commitInterval:5.0}")
    private double commitInterval;

    /** Max record changes before forcing an early commit (default 0 = unlimited). */
    @Value("${commitSizeLimit:0}")
    private int commitSizeLimit;

    @Value("${recceiver.cf.enabled:false}")
    private String cfEnabled;

    @Value("${recceiver.cf.service-url:}")
    private String cfServiceUrl;

    @Value("${recceiver.cf.username:}")
    private String cfUsername;

    @Value("${recceiver.cf.password:}")
    private String cfPassword;

    @Value("${recceiver.cf.delete-on-clean:false}")
    private String cfDeleteOnClean;

    @Value("${recceiver.cf.clean-on-startup:false}")
    private String cfCleanOnStartup;

    @Value("${recceiver.cf.clean-on-shutdown:true}")
    private String cfCleanOnShutdown;

    @Value("${recceiver.cf.alias-enabled:true}")
    private String cfAliasEnabled;

    @Value("${recceiver.cf.infotags:}")
    private String cfInfoTags;

    @Value("${recceiver.cf.environment-vars:}")
    private String cfEnvironmentVars;


    private final ScheduledExecutorService udpAnnouncerService = Executors.newScheduledThreadPool(1);
    private static int serverKey;
    private RecSyncProcessorDispatcher processorDispatcher;
    private volatile boolean running = true;

    private Selector selector;
    private ServerSocketChannel serverSocket;

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(RecceiverApplication.class, args);
        log.info("Starting Recceiver");
        context.addApplicationListener(event -> log.fine(event.toString()));
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting running");
        processorDispatcher = RecSyncProcessorRegistry.loadDispatcher(buildProcessorOptions());

        Random rand = new Random();
        serverKey = rand.nextInt();

        selector = Selector.open();
        // Create the TCP listener
        serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress(tcpBindAddress, tcpPort));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);

        InetSocketAddress localAddress = (InetSocketAddress) serverSocket.getLocalAddress();
        int port = localAddress.getPort();

        log.info("Listening on : " + tcpBindAddress + ":" + port);

        RecceiverUDPAnnouncer announcer = new RecceiverUDPAnnouncer(serverKey, localAddress, udpBroadcastAddress, udpBroadcastPort);
        udpAnnouncerService.scheduleAtFixedRate(announcer, 0, Math.round(announceInterval), TimeUnit.SECONDS);
        log.info("started announcing...");

        try {
            while (running) {
                selector.select();
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter = selectedKeys.iterator();
                while (iter.hasNext()) {

                    SelectionKey key = iter.next();
                    if (log.isLoggable(Level.FINER)) {
                        log.finer("selection key valid=" + key.isValid() + " channel=" + key.channel());
                    }

                    if (key.isAcceptable()) {
                        register(serverSocket);
                        log.finer("accepted connection via selector=" + selector + " server=" + serverSocket);
                    }
                    iter.remove();
                }
            }
        } catch (ClosedSelectorException ignored) {
            // Selector is closed during shutdown; treat as normal termination.
        }

    }

    public static final char SERVER_GREET_MSG_ID = 0x8001;
    public static final char PING_MSG_ID = 0x8002;


    private void register(ServerSocketChannel serverSocket)
            throws IOException {

        SocketChannel client = serverSocket.accept();
        client.configureBlocking(true);

        // Run each client handler on its own virtual thread.
        Thread.ofVirtual().name("recceiver-client-", 0).start(() -> {
            MessageProcessor p = new MessageProcessor(client, Math.max(1L, Math.round(tcptimeout)), serverKey, processorDispatcher, readBufferSize);
            try {
                while (client.isOpen() && client.isConnected()) {
                    p.process();
                }
            } catch (IOException e) {
                p.shutdown(disconnectReason(e));
                try {
                    client.close();
                } catch (IOException ex) {
                    log.log(Level.WARNING, "Failed to close client channel", ex);
                }
                if (parseDisconnectEvents(e)) {
                    log.log(Level.INFO, "Connection closed by peer: {0}", e.getMessage());
                } else {
                    log.log(Level.WARNING, "Connection processing failed", e);
                }
            } finally {
                p.shutdown();
            }

        });
    }

    private static boolean parseDisconnectEvents(IOException e) {
        if (!(e instanceof SocketException)) {
            return false;
        }
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        String lower = message.toLowerCase();
        return lower.contains("connection reset") || lower.contains("broken pipe") || lower.contains("forcibly closed");
    }

    private static String disconnectReason(IOException e) {
        if (parseDisconnectEvents(e)) {
            return "connection reset";
        }
        String message = e.getMessage();
        if (message == null || message.isBlank()) {
            return "io error";
        }
        return "io error: " + message;
    }

    private Map<String, String> buildProcessorOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("recceiver.cf.enabled", cfEnabled);
        options.put("recceiver.cf.service-url", cfServiceUrl);
        options.put("recceiver.cf.username", cfUsername);
        options.put("recceiver.cf.password", cfPassword);
        options.put("recceiver.cf.delete-on-clean", cfDeleteOnClean);
        options.put("recceiver.cf.clean-on-startup", cfCleanOnStartup);
        options.put("recceiver.cf.clean-on-shutdown", cfCleanOnShutdown);
        options.put("recceiver.cf.alias-enabled", cfAliasEnabled);
        options.put("recceiver.cf.infotags", cfInfoTags);
        options.put("recceiver.cf.environment-vars", cfEnvironmentVars);
        return options;
    }

    /**
     * Called by Spring during application shutdown via @PreDestroy lifecycle.
     * Triggers cleanup of all registered RecSyncProcessors (e.g., CFRecSyncProcessor)
     * to handle IOC channel cleanup in ChannelFinder.
     */
    @PreDestroy
    public void onShutdown() {
        log.info("RecceiverApplication shutdown: calling destroy on all processors");
        running = false;

        // Wake the blocking select() call so the command runner can exit cleanly.
        if (selector != null) {
            selector.wakeup();
        }

        udpAnnouncerService.shutdown();

        try {
            if (processorDispatcher != null) {
                processorDispatcher.destroyAllProcessors();
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error during processor shutdown", e);
        }

        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
            if (selector != null) {
                selector.close();
            }
        } catch (IOException e) {
            log.log(Level.WARNING, "Error while closing network resources", e);
        }
    }

}
