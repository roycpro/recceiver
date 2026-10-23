package org.phoebus.recceiver.spi.cf;

import org.phoebus.recceiver.spi.AddRecordMessage;
import org.phoebus.recceiver.spi.ConnectionClosedEvent;
import org.phoebus.recceiver.spi.RecSyncProcessor;
import org.phoebus.recceiver.spi.RecSyncTransaction;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Publishes channel records from IOCs to a ChannelFinder service.
 *
 * <p><strong>Lifecycle:</strong>
 * <ol>
 *   <li><strong>Initialization ({@link #init(String, Map)}):</strong> Creates an HTTP store, parses
 *       cleanup preferences, and ensures required properties exist in ChannelFinder. On startup,
 *       applies cleanup to any existing active channels (configuration-driven).</li>
 *   <li><strong>Commit ({@link #commit(RecSyncTransaction)}):</strong> For each transaction:
 *       <ul>
 *         <li>Registers new IOCs in the shutdown registry (on initial=true).</li>
 *         <li>Tracks per-IOC record state for delete message resolution.</li>
 *         <li>Builds channels from primary records and aliases (if enabled).</li>
 *         <li>Updates channels in ChannelFinder.</li>
 *         <li>Resolves and cleans up deleted records using tracked state.</li>
 *       </ul>
 *   </li>
 *   <li><strong>Connection Loss ({@link #connectionClosed(ConnectionClosedEvent)}):</strong>
 *       Immediately marks all IOC channels as inactive (or deletes them, depending on
 *       {@link #deleteOnClean}).</li>
 *   <li><strong>Shutdown ({@link #destroy()}):</strong> If {@link #cleanOnShutdown} is true, either
 *       deletes all of this recceiver's channels (if {@link #deleteOnClean} is true) or marks its
 *       Active and Inactive channels Unknown. Otherwise leaves ChannelFinder untouched.</li>
 * </ol>
 * </p>
 *
 * <p><strong>Cleanup Preferences (Booleans):</strong>
 * <ul>
 *   <li>{@code recceiver.cf.delete-on-clean} (false by default): How every cleanup is applied:
 *       delete channels (true) or update their pvStatus (false). Applies when an IOC disconnects,
 *       and to startup and shutdown cleanup.</li>
 *   <li>{@code recceiver.cf.clean-on-startup} (false by default): On init, clean all of this
 *       recceiver's channels: delete them (delete-on-clean=true) or mark them Inactive
 *       (delete-on-clean=false). When false, startup does not touch existing channels.</li>
 *   <li>{@code recceiver.cf.clean-on-shutdown} (true by default): On application shutdown, clean
 *       all of this recceiver's channels: delete them (delete-on-clean=true) or mark them Unknown
 *       (delete-on-clean=false). When false, shutdown does not touch ChannelFinder.</li>
 * </ul>
 * </p>
 *
 * <p><strong>Channel State Tracking:</strong>
 * <ul>
 *   <li>{@code iocRegistry}: Maps iocId → IocInfo for all connected IOCs. Used during shutdown to
 *       clean up tracked IOCs.</li>
 *   <li>{@code iocChannelState}: Maps iocId → per-IOC record name maps. Allows delete messages
 *       (which carry only recId, not channel names) to be resolved to actual channel names.</li>
 * </ul>
 * </p>
 *
 * <p><strong>Configuration:</strong>
 * <ul>
 *   <li>{@code recceiver.cf.enabled} (false by default): Enable this processor.</li>
 *   <li>{@code recceiver.cf.service-url} (required if enabled): URL to the ChannelFinder service.</li>
 *   <li>{@code recceiver.cf.username} / {@code recceiver.cf.password} (optional): HTTP basic auth.</li>
 *   <li>{@code recceiver.cf.alias-enabled} (false by default): Publish alias records as separate channels.</li>
 *   <li>{@code recceiver.cf.infotags} (optional): Comma/space-separated list of info tag names to
 *       publish as ChannelFinder properties.</li>
 *   <li>{@code recceiver.cf.environment-vars} (optional): Comma-separated mappings like
 *       "RSRV_SERVER_PORT:caPort" to map environment variables to properties.</li>
 * </ul>
 * </p>
 */
public class CFRecSyncProcessor implements RecSyncProcessor {
    private static final Logger log = Logger.getLogger(CFRecSyncProcessor.class.getName());
    private static final String ENABLED_KEY = "recceiver.cf.enabled";
    private static final String SERVICE_URL_KEY = "recceiver.cf.service-url";
    private static final String USERNAME_KEY = "recceiver.cf.username";
    private static final String PASSWORD_KEY = "recceiver.cf.password";
    private static final String DELETE_ON_CLEAN_KEY = "recceiver.cf.delete-on-clean";
    private static final String CLEAN_ON_STARTUP_KEY = "recceiver.cf.clean-on-startup";
    private static final String CLEAN_ON_SHUTDOWN_KEY = "recceiver.cf.clean-on-shutdown";
    private static final String ALIAS_ENABLED_KEY = "recceiver.cf.alias-enabled";
    private static final String INFOTAGS_KEY = "recceiver.cf.infotags";
    private static final String ENVIRONMENT_VARS_KEY = "recceiver.cf.environment-vars";

    // Inbound keys read from RecSync transaction client info / environment fields.
    // These names match protocol/env key spellings.
    public static final String IOCNAME_KEY = "IOCNAME";
    public static final String HOSTNAME_KEY = "HOSTNAME";
    public static final String ENGINEER_KEY = "ENGINEER";
    public static final String CF_USERNAME_KEY = "CF_USERNAME";
    public static final String CA_PORT_ENV_KEY = "RSRV_SERVER_PORT";
    public static final String PVA_PORT_ENV_KEY = "PVAS_SERVER_PORT";

    // ChannelFinder persisted property names written into channel documents.
    // These values follow ChannelFinder property names,
    // (for example: iocid, iocName, hostName).
    public static final String HOST_NAME_PROPERTY = "hostName";
    public static final String IOC_NAME_PROPERTY = "iocName";
    public static final String IOC_ID_PROPERTY = "iocid";
    public static final String IOC_IP_PROPERTY = "iocIP";
    public static final String PV_STATUS_PROPERTY = "pvStatus";
    public static final String TIME_PROPERTY = "time";
    public static final String RECCEIVER_ID_PROPERTY = "recceiverID";
    public static final String RECORD_TYPE_PROPERTY = "recordType";
    public static final String ALIAS_PROPERTY = "alias";
    public static final String CA_PORT_PROPERTY = "caPort";
    public static final String PVA_PORT_PROPERTY = "pvaPort";
    public static final String ACTIVE = "Active";
    public static final String INACTIVE = "Inactive";
    public static final String UNKNOWN = "Unknown";
    public static final String DEFAULT_OWNER = "cfstore";

    private static final String RECCEIVER_ID = resolveRecceiverId();

    private static final Set<String> REQUIRED_PROPERTIES = Set.copyOf(new LinkedHashSet<>(Set.of(
            HOST_NAME_PROPERTY,
            IOC_NAME_PROPERTY,
            PV_STATUS_PROPERTY,
            TIME_PROPERTY,
            IOC_ID_PROPERTY,
            IOC_IP_PROPERTY,
            RECCEIVER_ID_PROPERTY,
            RECORD_TYPE_PROPERTY,
            CA_PORT_PROPERTY,
            PVA_PORT_PROPERTY
    )));

    private ChannelFinderStore store;
    private boolean deleteOnClean;
    private boolean cleanOnStartup;
    private boolean cleanOnShutdown;
    private boolean aliasEnabled;
    private Set<String> infoTagWhitelist = Set.of();
    private Map<String, String> environmentVarMappings = Map.of();

    // IOC connection registry: tracks all IOCs that have connected to this processor.
    // Key = iocId (e.g., "host:port"), Value = IocInfo metadata
    private final Map<String, IocInfo> iocRegistry = new ConcurrentHashMap<>();
    // Tracks recId -> channel names so delete messages (which only carry recId) can be resolved.
    private final Map<String, IocChannelState> iocChannelState = new ConcurrentHashMap<>();

    public CFRecSyncProcessor() {
        // Required by ServiceLoader.
    }

    @Override
    public RecSyncProcessor init(String name, Map<String, String> options) {
        if (!isEnabled(options)) {
            return null;
        }
        String serviceUrl = options.get(SERVICE_URL_KEY);
        if (serviceUrl == null || serviceUrl.isBlank()) {
            throw new IllegalArgumentException("recceiver.cf.service-url must be configured");
        }

        String username = normalized(options.get(USERNAME_KEY));
        String password = normalized(options.get(PASSWORD_KEY));
        if ((username == null) != (password == null)) {
            throw new IllegalArgumentException(
                    "recceiver.cf.username and recceiver.cf.password must both be configured when authentication is used");
        }

        this.store = createStore(serviceUrl, username, password);
        this.deleteOnClean = Boolean.parseBoolean(options.getOrDefault(DELETE_ON_CLEAN_KEY, "false"));
        this.cleanOnStartup = Boolean.parseBoolean(options.getOrDefault(CLEAN_ON_STARTUP_KEY, "false"));
        this.cleanOnShutdown = Boolean.parseBoolean(options.getOrDefault(CLEAN_ON_SHUTDOWN_KEY, "true"));
        this.aliasEnabled = Boolean.parseBoolean(options.getOrDefault(ALIAS_ENABLED_KEY, "false"));
        this.infoTagWhitelist = parseInfoTags(options.get(INFOTAGS_KEY));
        // Parse once at init so commit path only performs lookups.
        this.environmentVarMappings = parseEnvironmentVarMappings(options.get(ENVIRONMENT_VARS_KEY));
        String propertyOwner = username == null ? DEFAULT_OWNER : username;
        Set<String> requiredProperties = new LinkedHashSet<>(REQUIRED_PROPERTIES);
        if (aliasEnabled) {
            requiredProperties.add(ALIAS_PROPERTY);
        }
        requiredProperties.addAll(infoTagWhitelist);
        requiredProperties.addAll(environmentVarMappings.values());
        this.store.ensureProperties(requiredProperties, propertyOwner);

        if (cleanOnStartup) {
            cleanupExistingActiveChannelsForRecceiver();
        }

        return this;
    }

    /**
     * On startup, cleans up any no inactive channels previously published by this recceiver instance.
     */
    private void cleanupExistingActiveChannelsForRecceiver() {
        Map<String, ChannelFinderChannel> existing = store.findByProperty(RECCEIVER_ID_PROPERTY, RECCEIVER_ID);
        cleanChannels(existing, INACTIVE);
    }

    /**
     * Filter channels whose pvStatus is NOT IN {@code excludedStatuses}.
     *
     * @param excludedStatuses pvStatus values to skip; empty to return every channel.
     */
    private Map<String, ChannelFinderChannel> filterChannelsExcluding(Map<String, ChannelFinderChannel> channels,
                                                                      Set<String> excludedStatuses) {
        if (excludedStatuses.isEmpty()) {
            return channels;
        }
        Map<String, ChannelFinderChannel> filtered = new LinkedHashMap<>();
        for (Map.Entry<String, ChannelFinderChannel> channelEntry : channels.entrySet()) {
            String status = channelEntry.getValue().property(PV_STATUS_PROPERTY);
            if (status == null || !excludedStatuses.contains(status)) {
                filtered.put(channelEntry.getKey(), channelEntry.getValue());
            }
        }
        return filtered;
    }

    /**
     * Applies the configured cleanup to the given channels: deletes them if {@link #deleteOnClean}
     * is true, otherwise sets their pvStatus to {@code statusIfKept} and refreshes their timestamp.
     *
     * @param channels channels to clean, keyed by name.
     * @param statusIfKept pvStatus to write when channels are kept rather than deleted.
     */
    private void cleanChannels(Map<String, ChannelFinderChannel> channels, String statusIfKept) {
        if (channels.isEmpty()) {
            return;
        }
        if (deleteOnClean) {
            store.deleteChannels(channels.keySet());
            return;
        }
        Map<String, ChannelFinderChannel> filteredChannels = filterChannelsExcluding(channels, Collections.singleton(statusIfKept));
        String now = OffsetDateTime.now().toString();
        Collection<ChannelFinderChannel> updated = new ArrayList<>();
        for (ChannelFinderChannel channel : filteredChannels.values()) {
            Map<String, String> properties = new LinkedHashMap<>(channel.properties());
            properties.put(PV_STATUS_PROPERTY, statusIfKept);
            properties.put(TIME_PROPERTY, now);
            updated.add(new ChannelFinderChannel(channel.name(), channel.owner(), properties));
        }
        store.updateChannels(updated);
    }

    protected ChannelFinderStore createStore(String serviceUrl, String username, String password) {
        return username == null
                ? new ChannelFinderHttpStore(serviceUrl)
                : new ChannelFinderHttpStore(serviceUrl, username, password);
    }

    /**
     * Processes a transaction from an IOC, updating channel records in ChannelFinder.
     *
     * <p><strong>On initial=true (first commit):</strong> Registers the IOC in the IOC registry.</p>
     *
     * <p><strong>On connected=false:</strong> Immediately triggers cleanup for this IOC's channels
     * by calling {@link #connectionClosed(ConnectionClosedEvent)}. This allows graceful disconnect
     * signaling within a transaction to be handled immediately.</p>
     *
     * <p><strong>Normal flow:</strong>
     * <ol>
     *   <li>Tracks per-IOC record state (recId → name mappings) for delete resolution.</li>
     *   <li>Builds channels from primary records and aliases using the transaction metadata.</li>
     *   <li>Updates all channels in ChannelFinder.</li>
     *   <li>Resolves and cleans up records marked for deletion (using tracked state to map recId → names).</li>
     * </ol>
     * </p>
     *
     * @param transaction the transaction containing records, deletes, and metadata.
     */
    @Override
    public void commit(RecSyncTransaction transaction) {
        log.info(() -> "[CFRecSyncProcessor] commit source=" + transaction.sourceAddress()
                + " initial=" + transaction.initial()
                + " connected=" + transaction.connected()
                + " adds=" + transaction.recordsToAdd().size()
                + " aliases=" + transaction.aliases().size()
                + " deletes=" + transaction.recordsToDelete().size()
                + " recordInfos=" + transaction.recordInfosToAdd().size()
                + " clientInfos=" + transaction.clientInfos().size());

        String iocId = resolveIocId(transaction);

        // Register this IOC in the in-memory IOC registry.
        if (transaction.initial()) {
            if (iocId != null && !iocId.isBlank()) {
                Map<String, String> clientInfos = transaction.clientInfos();
                IocInfo iocInfo = new IocInfo(
                        iocId,
                        transaction.sourceAddress(),
                        transaction.sourceHost(),
                        transaction.sourcePort(),
                        normalized(clientInfos.get(IOCNAME_KEY)),
                        resolveHostName(transaction, clientInfos),
                        resolveOwner(clientInfos),
                        OffsetDateTime.now().toString()
                );
                iocRegistry.put(iocId, iocInfo);
                log.info(() -> "[CFRecSyncProcessor] registered IOC: " + iocId);
            }
        }

        if (iocId != null && !iocId.isBlank()) {
            trackIocRecordState(iocId, transaction);
        }

        // A disconnected transaction indicates the IOC is offline; clean all IOC channels.
        if (iocId != null && !iocId.isBlank() && !transaction.connected()) {
            connectionClosed(new ConnectionClosedEvent(transaction.sourceAddress(), "commit connected=false"));
            return;
        }

        Collection<ChannelFinderChannel> channels = buildChannels(transaction);
        if (!channels.isEmpty()) {
            store.updateChannels(channels);
        }

        if (iocId != null && !iocId.isBlank()) {
            cleanupDeletedRecordChannels(iocId, transaction.recordsToDelete());
        }
    }

    /**
     * Tracks per-IOC record state (recId → names) for delete message resolution.
     *
     * <p>Populates two maps:
     * <ul>
     *   <li>{@code primaryRecordNamesByRecId}: Maps each primary record's recId to its name.</li>
     *   <li>{@code aliasNamesByRecId}: Maps each record's recId to its list of aliases.</li>
     * </ul>
     * </p>
     *
     * <p>When delete messages arrive (which contain only recIds), this state allows mapping from
     * recId back to the actual channel names to delete.</p>
     *
     * @param iocId the IOC's unique identifier (typically host:port).
     * @param transaction the transaction containing current records and aliases.
     */
    private void trackIocRecordState(String iocId, RecSyncTransaction transaction) {
        IocChannelState state = iocChannelState.computeIfAbsent(iocId, ignored -> new IocChannelState());
        for (AddRecordMessage addRecordMessage : transaction.recordsToAdd().values()) {
            if (addRecordMessage.aType() == 0) {
                state.primaryRecordNamesByRecId.put(addRecordMessage.recId(), addRecordMessage.recordName());
            }
        }
        for (Map.Entry<Integer, List<String>> aliasEntry : transaction.aliases().entrySet()) {
            List<String> aliases = aliasEntry.getValue().stream()
                    .filter(alias -> alias != null && !alias.isBlank())
                    .toList();
            if (!aliases.isEmpty()) {
                state.aliasNamesByRecId.put(aliasEntry.getKey(), aliases);
            }
        }
    }

    /**
     * Resolves and cleans up deleted records using the tracked per-IOC state.
     *
     * <p>For each deleted recId, looks up the primary record name and aliases using the IOC's
     * tracked state. Finds existing channels in ChannelFinder and either deletes them or marks
     * them as inactive, per the {@link #deleteOnClean} preference.</p>
     *
     * @param iocId the IOC's unique identifier.
     * @param deletedRecIds the set of recIds to clean up.
     */
    private void cleanupDeletedRecordChannels(String iocId, Set<Integer> deletedRecIds) {
        if (deletedRecIds.isEmpty()) {
            return;
        }

        IocChannelState state = iocChannelState.get(iocId);
        if (state == null) {
            return;
        }

        Set<String> channelNamesToCleanup = new LinkedHashSet<>();
        for (Integer recId : deletedRecIds) {
            String primaryName = state.primaryRecordNamesByRecId.remove(recId);
            if (primaryName != null) {
                channelNamesToCleanup.add(primaryName);
            }
            List<String> aliases = state.aliasNamesByRecId.remove(recId);
            if (aliases != null) {
                channelNamesToCleanup.addAll(aliases);
            }
        }

        if (channelNamesToCleanup.isEmpty()) {
            return;
        }

        cleanChannels(store.findByNames(channelNamesToCleanup), INACTIVE);
    }

    private Collection<ChannelFinderChannel> buildChannels(RecSyncTransaction transaction) {
        Collection<ChannelFinderChannel> channels = new ArrayList<>();
        Map<String, String> clientInfos = transaction.clientInfos();
        Map<Integer, Map<String, String>> recordInfos = transaction.recordInfosToAdd();
        String owner = resolveOwner(clientInfos);
        // Group the commit-wide IOC/timestamp/port values once so primary and alias
        // channel builders reuse the same shared metadata.
        ChannelContext context = buildChannelContext(transaction, clientInfos);

        warnForOrphanInfoTags(transaction, recordInfos);

        addPrimaryChannels(transaction, recordInfos, owner, context, channels);

        if (aliasEnabled) {
            addAliasChannels(transaction, recordInfos, owner, context, channels);
        }

        return channels;
    }

    private static String resolveHostName(RecSyncTransaction transaction, Map<String, String> clientInfos) {
        String hostName = normalized(clientInfos.get(HOSTNAME_KEY));
        return hostName != null ? hostName : fallbackSourceHost(transaction);
    }

    private static String resolveIocName(RecSyncTransaction transaction, Map<String, String> clientInfos) {
        String iocName = normalized(clientInfos.get(IOCNAME_KEY));
        if (iocName != null) {
            return iocName;
        }
        return transaction.sourcePort() != null ? String.valueOf(transaction.sourcePort()) : transaction.sourceAddress();
    }

    private static String resolveIocId(RecSyncTransaction transaction) {
        return transaction.sourceHost() != null && transaction.sourcePort() != null
                ? transaction.sourceHost() + ":" + transaction.sourcePort()
                : transaction.sourceAddress();
    }

    private static String resolveIocIp(RecSyncTransaction transaction) {
        return fallbackSourceHost(transaction);
    }

    private static String fallbackSourceHost(RecSyncTransaction transaction) {
        return transaction.sourceHost() != null ? transaction.sourceHost() : transaction.sourceAddress();
    }

    private Map<String, String> buildProperties(
            AddRecordMessage addRecord,
            Map<String, String> recordInfoProperties,
            ChannelContext context
    ) {
        Map<String, String> properties = buildStandardProperties(
                addRecord,
                context
        );
        if (recordInfoProperties != null && !recordInfoProperties.isEmpty() && !infoTagWhitelist.isEmpty()) {
            for (Map.Entry<String, String> entry : recordInfoProperties.entrySet()) {
                if (infoTagWhitelist.contains(entry.getKey())) {
                    properties.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return properties;
    }

    private static Map<String, String> buildStandardProperties(AddRecordMessage addRecord, ChannelContext context) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(HOST_NAME_PROPERTY, context.hostName());
        properties.put(IOC_NAME_PROPERTY, context.iocName());
        properties.put(PV_STATUS_PROPERTY, ACTIVE);
        properties.put(TIME_PROPERTY, context.timestamp());
        properties.put(IOC_ID_PROPERTY, context.iocId());
        properties.put(IOC_IP_PROPERTY, context.iocIp());
        properties.put(RECCEIVER_ID_PROPERTY, RECCEIVER_ID);
        properties.put(RECORD_TYPE_PROPERTY, addRecord.recordType());
        if (context.caPort() != null) {
            properties.put(CA_PORT_PROPERTY, context.caPort());
        }
        if (context.pvaPort() != null) {
            properties.put(PVA_PORT_PROPERTY, context.pvaPort());
        }
        // Apply configured ENV->property mappings resolved from clientInfos for this commit.
        properties.putAll(context.mappedEnvironmentProperties());
        return properties;
    }

    private ChannelContext buildChannelContext(RecSyncTransaction transaction, Map<String, String> clientInfos) {
        return new ChannelContext(
                resolveHostName(transaction, clientInfos),
                resolveIocName(transaction, clientInfos),
                resolveIocId(transaction),
                resolveIocIp(transaction),
                OffsetDateTime.now().toString(),
                normalized(clientInfos.get(CA_PORT_ENV_KEY)),
                normalized(clientInfos.get(PVA_PORT_ENV_KEY)),
                buildMappedEnvironmentProperties(clientInfos)
        );
    }

    private Map<String, String> buildMappedEnvironmentProperties(Map<String, String> clientInfos) {
        if (environmentVarMappings.isEmpty()) {
            return Map.of();
        }

        Map<String, String> mappedProperties = new LinkedHashMap<>();
        for (Map.Entry<String, String> envMapping : environmentVarMappings.entrySet()) {
            String value = normalized(clientInfos.get(envMapping.getKey()));
            if (value != null) {
                mappedProperties.put(envMapping.getValue(), value);
            }
        }
        return Map.copyOf(mappedProperties);
    }

    private void warnForOrphanInfoTags(RecSyncTransaction transaction, Map<Integer, Map<String, String>> recordInfos) {
        for (Map.Entry<Integer, Map<String, String>> recordInfoEntry : recordInfos.entrySet()) {
            if (!transaction.recordsToAdd().containsKey(recordInfoEntry.getKey())) {
                log.warning(() -> "[CFRecSyncProcessor] info tags for recId " + recordInfoEntry.getKey()
                        + " have no matching primary record");
            }
        }
    }

    private void addAliasChannels(
            RecSyncTransaction transaction,
            Map<Integer, Map<String, String>> recordInfos,
            String owner,
            ChannelContext context,
            Collection<ChannelFinderChannel> channels
    ) {
        for (Map.Entry<Integer, List<String>> aliasEntry : transaction.aliases().entrySet()) {
            AddRecordMessage primaryRecord = transaction.recordsToAdd().get(aliasEntry.getKey());
            if (primaryRecord == null) {
                log.warning(() -> "[CFRecSyncProcessor] alias recId " + aliasEntry.getKey()
                        + " has no primary record in recordsToAdd");
                continue;
            }
            for (String aliasName : aliasEntry.getValue()) {
                if (aliasName == null || aliasName.isBlank()) {
                    continue;
                }
                Map<String, String> aliasProperties = buildProperties(
                        primaryRecord,
                        recordInfos.get(primaryRecord.recId()),
                        context
                );
                aliasProperties.put(ALIAS_PROPERTY, primaryRecord.recordName());
                channels.add(new ChannelFinderChannel(aliasName, owner, aliasProperties));
            }
        }
    }

    private void addPrimaryChannels(
            RecSyncTransaction transaction,
            Map<Integer, Map<String, String>> recordInfos,
            String owner,
            ChannelContext context,
            Collection<ChannelFinderChannel> channels
    ) {
        for (AddRecordMessage addRecord : transaction.recordsToAdd().values()) {
            if (addRecord.aType() != 0) {
                continue;
            }
            Map<String, String> properties = buildProperties(
                    addRecord,
                    recordInfos.get(addRecord.recId()),
                    context
            );
            channels.add(new ChannelFinderChannel(addRecord.recordName(), owner, properties));
        }
    }

    /**
     * Handles connection loss for an IOC, marking its channels as inactive or deleting them.
     *
     * <p>Resolves the IOC's ID from the source address, finds all channels associated with that IOC
     * (via the iocid property), and applies cleanup:
     * <ul>
     *   <li>If {@link #deleteOnClean} is true, deletes the channels.</li>
     *   <li>Otherwise, marks them as inactive with an updated timestamp.</li>
     * </ul>
     * </p>
     *
     * <p>Clears the IOC from both the registry and channel state map after cleanup.</p>
     *
     * @param event the connection closed event.
     */
    @Override
    public void connectionClosed(ConnectionClosedEvent event) {
        log.info(() -> "[CFRecSyncProcessor] connectionClosed source=" + event.sourceAddress()
                + " reason='" + event.reason() + "'");

        String iocId = resolveIocId(event.sourceAddress());
        if (iocId == null || iocId.isBlank()) {
            return;
        }

        Map<String, ChannelFinderChannel> existing = store.findByProperty(IOC_ID_PROPERTY, iocId);
        if (existing.isEmpty()) {
            return;
        }

        cleanChannels(existing, INACTIVE);
        iocRegistry.remove(iocId);
        iocChannelState.remove(iocId);
    }

    @Override
    public void destroy() {
        if (!cleanOnShutdown) {
            log.info("[CFRecSyncProcessor] destroy: clean-on-shutdown disabled, leaving ChannelFinder untouched");
            return;
        }
        try {
            Map<String, ChannelFinderChannel> channels = store.findByProperty(RECCEIVER_ID_PROPERTY, RECCEIVER_ID);
            log.info(() -> "[CFRecSyncProcessor] destroy: "
                    + (deleteOnClean ? "deleting " : "marking " + UNKNOWN + " ")
                    + channels.size() + " channels for recceiver " + RECCEIVER_ID);
            cleanChannels(channels, UNKNOWN);
        } catch (Exception e) {
            String errMsg = "[CFRecSyncProcessor] error during shutdown cleanup: " + e.getMessage();
            System.err.println(errMsg);
            log.severe(() -> errMsg);
        } finally {
            iocRegistry.clear();
            iocChannelState.clear();
        }
    }

    protected int iocRegistrySize() {
        return iocRegistry.size();
    }

    private static String resolveIocId(String sourceAddress) {
        if (sourceAddress == null || sourceAddress.isBlank()) {
            return null;
        }
        String candidate = sourceAddress.trim();
        if (candidate.startsWith("/")) {
            candidate = candidate.substring(1);
        }
        int colonIndex = candidate.lastIndexOf(':');
        if (colonIndex <= 0 || colonIndex >= candidate.length() - 1) {
            return candidate;
        }
        String hostPart = candidate.substring(0, colonIndex);
        String portPart = candidate.substring(colonIndex + 1);
        try {
            int port = Integer.parseInt(portPart);
            if (port < 0 || port > 65535) {
                return candidate;
            }
            int slashIndex = hostPart.lastIndexOf('/');
            String host = slashIndex >= 0 && slashIndex < hostPart.length() - 1
                    ? hostPart.substring(slashIndex + 1)
                    : hostPart;
            return host + ":" + port;
        } catch (NumberFormatException ignored) {
            return candidate;
        }
    }

    private static String resolveOwner(Map<String, String> clientInfos) {
        String engineer = normalized(clientInfos.get(ENGINEER_KEY));
        if (engineer != null) {
            return engineer;
        }
        String cfUsername = normalized(clientInfos.get(CF_USERNAME_KEY));
        if (cfUsername != null) {
            return cfUsername;
        }
        return DEFAULT_OWNER;
    }

    private static String resolveRecceiverId() {
        String fromHostName = normalized(System.getenv(HOSTNAME_KEY));
        if (fromHostName != null) {
            return fromHostName;
        }
        String fromComputerName = normalized(System.getenv("COMPUTERNAME"));
        if (fromComputerName != null) {
            return fromComputerName;
        }
        try {
            String localHost = normalized(InetAddress.getLocalHost().getHostName());
            if (localHost != null) {
                return localHost;
            }
        } catch (UnknownHostException ignored) {
            // Fall back to the default owner when the local host name cannot be resolved.
        }
        return DEFAULT_OWNER;
    }

    private static String normalized(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static Set<String> parseInfoTags(String configuredInfoTags) {
        String normalizedInfoTags = normalized(configuredInfoTags);
        if (normalizedInfoTags == null) {
            return Set.of();
        }

        Set<String> parsed = new LinkedHashSet<>();
        for (String token : normalizedInfoTags.split("[,\\s]+")) {
            String infoTag = normalized(token);
            if (infoTag != null) {
                parsed.add(infoTag);
            }
        }
        return Set.copyOf(parsed);
    }

    /**
     * Parses mappings in the format "ENV_VAR:PropertyName,OTHER_ENV:OtherProperty".
     * @param configuredMappings the raw input string containing environment variable to property name mappings
     * @return a map of environment variable name to channel property name for each valid mapping parsed from the input string
     */
    private static Map<String, String> parseEnvironmentVarMappings(String configuredMappings) {
        String normalizedMappings = normalized(configuredMappings);
        if (normalizedMappings == null) {
            return Map.of();
        }

        Map<String, String> parsed = new LinkedHashMap<>();
        for (String token : normalizedMappings.split(",")) {
            String mapping = normalized(token);
            if (mapping == null) {
                continue;
            }
            int separatorIndex = mapping.indexOf(':');
            if (separatorIndex <= 0 || separatorIndex >= mapping.length() - 1) {
                throw new IllegalArgumentException(
                        "Invalid environment variable mapping '" + mapping + "'. Expected format ENV:PropertyName");
            }
            String envVar = normalized(mapping.substring(0, separatorIndex));
            String propertyName = normalized(mapping.substring(separatorIndex + 1));
            if (envVar == null || propertyName == null) {
                throw new IllegalArgumentException(
                        "Invalid environment variable mapping '" + mapping + "'. Expected format ENV:PropertyName");
            }
            // Keep the last mapping if a variable appears multiple times.
            parsed.put(envVar, propertyName);
        }
        return Map.copyOf(parsed);
    }

    /**
     * Shared metadata collected from a transaction and copied into every channel emitted for that
     * transaction. This avoids repeating IOC metadata computation for each record.
     *
     * <p>Fields include:
     * <ul>
     *   <li>{@code hostName}: The IOC's host name (from HOSTNAME clientInfo or fallback to sourceHost).</li>
     *   <li>{@code iocName}: The IOC's logical name (from IOCNAME clientInfo or fallback to port/sourceAddress).</li>
     *   <li>{@code iocId}: Unique IOC identifier (typically host:port).</li>
     *   <li>{@code iocIp}: The IOC's IP address (from sourceHost or fallback to sourceAddress).</li>
     *   <li>{@code timestamp}: Transaction commit time in ISO-8601 format.</li>
     *   <li>{@code caPort}, {@code pvaPort}: Optional channel access and PVA server ports.</li>
     *   <li>{@code mappedEnvironmentProperties}: Properties derived from configured environment variable mappings.</li>
     * </ul>
     * </p>
     */
    private record ChannelContext(
            String hostName,
            String iocName,
            String iocId,
            String iocIp,
            String timestamp,
            String caPort,
            String pvaPort,
            Map<String, String> mappedEnvironmentProperties
    ) {
    }

    /**
     * Metadata about a connected IOC, kept in the in-memory IOC registry.
     *
     *
     * <p>Fields include source address, host, port, IOC name, host name, owner, and connection timestamp.</p>
     */
    private record IocInfo(
            String iocId,
            String sourceAddress,
            String sourceHost,
            Integer sourcePort,
            String iocName,
            String hostName,
            String owner,
            String connectedAt
    ) {
    }

    /**
     * Per-IOC record state tracking for delete message resolution.
     *
     * <p>Maintains two maps:
     * <ul>
     *   <li>{@code primaryRecordNamesByRecId}: recId → primary record name for each record.</li>
     *   <li>{@code aliasNamesByRecId}: recId → list of alias names (if any).</li>
     * </ul>
     * </p>
     *
     * <p>When a delete message arrives with only a recId, this state allows the processor to look
     * up which primary record and aliases to delete from ChannelFinder.</p>
     */
    private static final class IocChannelState {
        private final Map<Integer, String> primaryRecordNamesByRecId = new LinkedHashMap<>();
        private final Map<Integer, List<String>> aliasNamesByRecId = new LinkedHashMap<>();
    }

    private static boolean isEnabled(Map<String, String> options) {
        String fromOptions = options.get(ENABLED_KEY);
        if (fromOptions != null && !fromOptions.isBlank()) {
            return Boolean.parseBoolean(fromOptions);
        }

        String fromProperty = System.getProperty(ENABLED_KEY);
        if (fromProperty != null && !fromProperty.isBlank()) {
            return Boolean.parseBoolean(fromProperty);
        }

        String fromEnv = System.getenv("RECCEIVER_CF_ENABLED");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return Boolean.parseBoolean(fromEnv);
        }

        return false;
    }
}
