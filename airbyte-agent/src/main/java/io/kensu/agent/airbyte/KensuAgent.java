package io.kensu.agent.airbyte;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.JsonNode;

import io.airbyte.commons.text.Names;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.DefaultAirbyteSource;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.DefaultAirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.MessageTracker;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.WorkerMetricReporter;

import io.kensu.dam.*;
import io.kensu.dam.model.*;
import io.kensu.dam.model.Process;
import io.kensu.dam.util.Compact;

public class KensuAgent {
    private static final Logger LOGGER = LoggerFactory.getLogger(KensuAgent.class);
    
    public static String PROPERTIES_FILE = "/kensu.properties";

    public String serverHost;

    public AirbyteSource source;
    public String sourceImage = null;
    public DataSource sourceDS = null;
    public Schema sourceSC = null;
    public Map<String, Double> sourceMetrics = new HashMap<>();
    
    public AirbyteDestination destination;
    public String destinationImage = null;
    public DataSource destinationDS = null;
    public Schema destinationSC = null;
    public Map<String, Double> destinationMetrics = new HashMap<>();

    public AirbyteMapper mapper;
    public ProcessLineage lineage = null;
    public LineageRun lineageRun = null;

    public MessageTracker messageTracker;
    public WorkerMetricReporter metricReporter;

    public RecordSchemaValidator recordSchemaValidator;
    public Map<String, JsonNode> recordSchemaValidatorStreams;

    public Project project;
    public Process process;
    public ProcessRun processRun;
    public User launchingUser;
    public User maintainerUser;
    public CodeBase codeBase;
    public CodeVersion codeVersion;

    public ManageKensuDamEntitiesApi observationsAPI;
    public Properties configuration = new Properties();

    public KensuAgent(AirbyteSource source, AirbyteDestination destination, AirbyteMapper mapper,
            MessageTracker messageTracker, RecordSchemaValidator recordSchemaValidator,
            WorkerMetricReporter metricReporter) {

        InputStream resourceStream = getClass().getResourceAsStream(PROPERTIES_FILE);
        if (resourceStream != null) {
            try {
                configuration.load(resourceStream);
            } catch (IOException e) {
                LOGGER.error("Cannot load properties file: " + PROPERTIES_FILE, e);
                configuration = null;
            }
        } else {
            LOGGER.error("Cannot access properties file: " + PROPERTIES_FILE);
            configuration = null;
        }

        ApiClient apiClient = null;
        if (configuration == null || (configuration.getProperty("kensu.offline.enabled") != null && 
                                        Boolean.parseBoolean(configuration.getProperty("kensu.offline.enabled")))) {
            LOGGER.info("Using offline mode for observations");
            // file path to store observations
            String filePath = (configuration==null)?null:configuration.getProperty("kensu.offline.file");
            // Offline client
            apiClient = new OfflineFileApiClient(filePath);
        } else {
            LOGGER.info("Using online mode for observations");
            String apiHost = configuration.getProperty("kensu.api");
            String authToken = configuration.getProperty("kensu.auth_token"); // TODO => how to have a token per process?
            // Online
            apiClient = new ApiClient()
                    .setBasePath(apiHost)
                    .addDefaultHeader("X-Auth-Token", authToken);
        }
        this.observationsAPI = new ManageKensuDamEntitiesApi(apiClient);
        // ensuring the default PhysicalLocation is reported (needed for DataSources)
        try {
            this.observationsAPI.reportPhysicalLocation(UNKNOWN_PL);
        } catch (ApiException e) {
            LOGGER.error("Cannot report physical location", e);
        }

        this.source = source;
        if (source == null) {
            LOGGER.error("Source cannot be null");
        } else if (!(source instanceof DefaultAirbyteSource)) {
            LOGGER.error("Cannot process Source of type: " + source.getClass().getName());
            this.source = null;
        } else {
            // from source_specs.yaml
            this.sourceImage = KensuAgent.getImageNameFromAirbyteSourceOrDestination(source);
        }
        this.destination = destination;
        if (destination == null) {
            LOGGER.error("Destination cannot be null");
        } else if (!(destination instanceof DefaultAirbyteDestination)) {
            LOGGER.error("Cannot process Destination of type: " + destination.getClass().getName());
            this.destination = null;
        } else {
            // from source_specs.yaml
            this.destinationImage = KensuAgent.getImageNameFromAirbyteSourceOrDestination(destination);
        }
        this.mapper = mapper;
        this.messageTracker = messageTracker;
        this.metricReporter = metricReporter;
        this.recordSchemaValidator = recordSchemaValidator;
        if (recordSchemaValidator != null) {
            // problem => `streams` is private
            this.recordSchemaValidatorStreams = KensuAgent.<Map<String, JsonNode>>getPrivateField(RecordSchemaValidator.class, "streams", recordSchemaValidator);
        }

        if (sourceImage != null && destinationImage != null) {
            // We make the choice that each sync is a process, not the Airbyte Server
            // A process is currently representing the link between a Source type (docker) and destination type (docker)
            // TODO => should it be coming from the syncInput.catalog.streams?
            this.process = new Process().pk(new ProcessPK().qualifiedName("Airbyte:" + sourceImage + "->"+ destinationImage));
            this.project = new Project().pk(new ProjectPK().name("Airbyte")); //TODO Provided By Airbyte?
            this.launchingUser = new User().pk(new UserPK().name("AirbyteLauncher")); //TODO Provided by Airbyte?
            this.codeBase = new CodeBase().pk(new CodeBasePK().location("AirbyteCodeBase"));  //TODO Provided by Airbyte?
            this.maintainerUser = new User().pk(new UserPK().name("AirbyteMaintainer")); //TODO Provided by Airbyte?
            this.codeVersion = new CodeVersion().pk(new CodeVersionPK().version("AirbyteCodeVersion") // TODO Provided by Airbyte?
                                                                        .codebaseRef(new CodeBaseRef().byPK(this.codeBase.getPk())))
                                                .maintainersRefs(new ArrayList<>(List.of(new UserRef().byPK(this.maintainerUser.getPk()))));
            this.processRun = new ProcessRun().projectsRefs(new ArrayList<>(List.of(new ProjectRef().byPK(this.project.getPk())))) 
                                                .launchedByUserRef(new UserRef().byPK(launchingUser.getPk()))
                                                .executedCodeVersionRef(new CodeVersionRef().byPK(this.codeVersion.getPk()))
                                                .environment("Production") //TODO Provided by Airbyte?
                                                .pk(new ProcessRunPK().qualifiedName(process.getPk().getQualifiedName())
                                                                        .processRef(new ProcessRef().byPK(process.getPk()))); 
            // sending observations   
            try {
                observationsAPI.reportProcess(process);
                observationsAPI.reportProject(project);
                observationsAPI.reportUser(launchingUser);
                observationsAPI.reportCodeBase(codeBase);
                observationsAPI.reportUser(maintainerUser);
                observationsAPI.reportCodeVersion(codeVersion);
                observationsAPI.reportProcessRun(processRun);
            } catch (ApiException e) {
                LOGGER.error("Cannot send process, project, user, codeBase, and/or codeVersion", e);
            }
        }
    }
    
    private static <R> R getPrivateField(Class cl, String fieldName, Object o) {
        try {
            Field field = cl.getDeclaredField(fieldName);
            // Set the accessibility as true
            field.setAccessible(true);
            // Return the value of private field in variable
            return (R)field.get(o);
        } catch (java.lang.NoSuchFieldException e) {
            LOGGER.error("Cannot find field: " + fieldName + " in class: " + cl.getName() + " for object:" + o + ". Error: " + e.getMessage());
            e.printStackTrace();
            return null;
        } catch (java.lang.IllegalAccessException e) {
            LOGGER.error("Cannot access field: " + fieldName + " in class: " + cl.getName() + " for object:" + o + ". Error: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private static <T> String getImageNameFromAirbyteSourceOrDestination(T sourceOrDestination) {
        // problem => `integrationLauncher` is private
        IntegrationLauncher launcher = KensuAgent.<IntegrationLauncher>getPrivateField(sourceOrDestination.getClass(), "integrationLauncher", sourceOrDestination);
        if (launcher == null) {
            LOGGER.error("Cannot fetch image name from null integration launcher");
            return null;
        }
        if (launcher instanceof AirbyteIntegrationLauncher) {
            // problem => `imageName` is private
            return KensuAgent.<String>getPrivateField(launcher.getClass(), "imageName", launcher);
        } else {
            LOGGER.error("Cannot fetch image name from integration launcher of type: " + launcher.getClass().getName());
            return null;
        }
    }

    private Map.Entry<Predicate<String>, Consumer<StandardSyncInput>> createProcessEntry(String matchStart, Consumer<StandardSyncInput> process) {
        return Map.<Predicate<String>, Consumer<StandardSyncInput>>entry((image) -> image.startsWith(matchStart), process);
    }

    // TODO could be filled reading the specs YAML
    public Map<Predicate<String>, Consumer<StandardSyncInput>> sources = Map.ofEntries(
        createProcessEntry("airbyte/source-file", (syncInput) -> this.processAirbyteSourceFile(syncInput))
    );
    public Map<Predicate, Consumer<StandardSyncInput>> destinations = Map.ofEntries(
        createProcessEntry("airbyte/destination-csv", (syncInput) -> this.processAirbyteDestinationCsv(syncInput))
    );

    public void init(StandardSyncInput syncInput) {
        if (sourceImage == null) {
            LOGGER.error("Cannot proceed as source image name is unknown");
            return;
        } else {
            Optional<Consumer<StandardSyncInput>> sourceProcessor = sources.entrySet().stream().filter(e->e.getKey().test(sourceImage))
                                                                                                    .map(e->e.getValue())
                                                                                                    .findFirst();
            if (sourceProcessor.isPresent()) {
                sourceProcessor.get().accept(syncInput);
            } else {
                LOGGER.error("Cannot handle source image: " + sourceImage);
            }
        }
        if (destinationImage == null ) {
            LOGGER.error("Cannot proceed as destination image name is unknown");
            return;
        } else {
            Optional<Consumer<StandardSyncInput>> destinationProcessor = destinations.entrySet().stream().filter(e->e.getKey().test(destinationImage))
                                                                                                        .map(e->e.getValue())
                                                                                                        .findFirst();
            if (destinationProcessor.isPresent()) {
                destinationProcessor.get().accept(syncInput);
            } else {
                LOGGER.error("Cannot handle destination image: " + destinationImage);
            }
        }
    }

    // SOURCE PROCESSORS
    private void processAirbyteSourceFile(StandardSyncInput syncInput) {
        // configured source for the sync
        JsonNode sourceConfiguration = syncInput.getSourceConfiguration();
        // "https://www.donneesquebec.ca/recherche/fr/dataset/857d007a-f195-434b-bc00-7012a6244a90/resource/16f55019-f05d-4375-a064-b75bce60543d/download/pf-mun-2019-2019.csv"
        String url = sourceConfiguration.get("url").textValue();
        // "csv"
        String format = sourceConfiguration.get("format").textValue();
        // "HTTPS"
        String providerStorage = sourceConfiguration.get("provider").get("storage").textValue();
        // "donneesquebec"
        String datasetName = sourceConfiguration.get("dataset_name").textValue();

        sourceDS = new DataSource()
                .name(datasetName)
                .format(format)
                .pk(new DataSourcePK()
                        .location(url)
                        .physicalLocationRef(UNKNOWN_PL_REF));

        // validator => get schema => configured
        if (recordSchemaValidatorStreams != null) {
            SchemaPK pk = new SchemaPK().dataSourceRef(new DataSourceRef().byPK(this.sourceDS.getPk()));
            JsonNode sourceJsonSchema = recordSchemaValidatorStreams.get(datasetName);
            // properties: {"field1": {"type": ["string", "null"]}, ...}
            // properties.fields: Iterator<Map.Entry<String,JsonNode>>
            Iterator<Map.Entry<String, JsonNode>> itFields = sourceJsonSchema.get("properties").fields();
            while (itFields.hasNext()) {
                Map.Entry<String, JsonNode> field = itFields.next();
                String fieldName = field.getKey();
                String fieldType = field.getValue().get("type").elements().next().textValue();
                // TODO better typing => we get `number` and `string`
                Boolean fieldNullable = true; // TODO... no idea?
                pk = pk.addFieldsItem(new FieldDef().name(fieldName).fieldType(fieldType).nullable(fieldNullable));
            }
            sourceSC = new Schema().name(datasetName).pk(pk);
        } else {
            LOGGER.warn("No schema validator available, so no schema available for source: " + source);
        }
    }

    // DESTINATION PROCESSORS
    private void processAirbyteDestinationCsv(StandardSyncInput syncInput) {
        // configured destination for the sync
        JsonNode destinationConfiguration = syncInput.getDestinationConfiguration();
        //"/tmp"
        String destinationPath = destinationConfiguration.get("destination_path").textValue();
        // FIXME => always only 1?
        ConfiguredAirbyteStream stream = syncInput.getCatalog().getStreams().get(0);
        String streamName = stream.getStream().getName();
        // shame... copied from the Destination's code. This could be available somewhere else, or differently 
        String fileName = Names.toAlphanumericAndUnderscore("_airbyte_raw_" + streamName);
        String rootPathForDestinationCsv = System.getenv("LOCAL_ROOT");
        if (rootPathForDestinationCsv == null) {
            // default...
            rootPathForDestinationCsv = "/tmp/airbyte_local";
        }
        String fileLocation = rootPathForDestinationCsv + "/" + destinationPath + "/" + fileName;
        destinationDS = new DataSource()
                            .name(fileName)
                            .format("csv")
                            .pk(new DataSourcePK()
                                .location(fileLocation) 
                                .physicalLocationRef(UNKNOWN_PL_REF));
        destinationSC = new Schema().name(destinationDS.getName()).pk(new SchemaPK().dataSourceRef(new DataSourceRef().byPK(this.destinationDS.getPk())));
    }

    // Message HANDLERS (read, map, write)

    public void handleMessageMapped(AirbyteMessage message) {
        if (destinationSC != null) {
            AirbyteRecordMessage record = message.getRecord();
            JsonNode data = record.getData();
            Iterator<Map.Entry<String, JsonNode>> itFields = data.fields();
            while (itFields.hasNext()) {
                Map.Entry<String, JsonNode> field = itFields.next();
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                Optional<FieldDef> of = destinationSC.getPk().getFields().stream().filter(f -> f.getName().equals(fieldName)).findFirst();
                if (!of.isPresent() && !fieldValue.isNull()) {
                    //update destination schema
                    String fieldType = null;
                    // TODO better typing
                    if (fieldValue.isNumber()) {
                        fieldType = "number";
                    } else if (fieldValue.isTextual()) {
                        fieldType = "string";
                    } else {
                        LOGGER.debug("Not handled mapped message field type: " + fieldValue.getNodeType());
                    }
                    if (fieldType != null) {
                        destinationSC.getPk().addFieldsItem(new FieldDef().name(fieldName).fieldType(fieldType).nullable(true));
                    }
                }
                // TODO need to add "unknown" type for fields which for all message have only null values! 
                //      As it won't be added in the schema then
            }
        } else {
            LOGGER.warn("Process mapped message skipped as Destination schema is null, see logs from `init`");
        }
    }

    public void handleMessageRead(Optional<AirbyteMessage> message) {
        // TODO update schema before it is validated -- to observe missing stuff and alike
        if (message.isPresent()) {
            AirbyteRecordMessage record = message.get().getRecord();
            JsonNode data = record.getData();
            Iterator<Map.Entry<String, JsonNode>> itFields = data.fields();
            while (itFields.hasNext()) {
                Map.Entry<String, JsonNode> field = itFields.next();
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                String fieldType = null;
                if (fieldValue.isNumber()) {
                    fieldType = "number";
                } else if (fieldValue.isTextual()) {
                    fieldType = "string";
                } else {
                    LOGGER.debug("Not handled read message field type to accumulate metrics: " + fieldValue.getNodeType());
                }
                if (fieldType != null) {
                    updateMetrics(sourceMetrics, fieldName, fieldType, fieldValue);
                }
            }
            sourceMetrics.compute("count", (k,v) -> (v==null)?1:v+1);
        } else {
            // TODO... what do we do here?
        }
    }

    public void handleMessageCopied(AirbyteMessage message) {
        AirbyteRecordMessage record = message.getRecord();
        JsonNode data = record.getData();
        Iterator<Map.Entry<String, JsonNode>> itFields = data.fields();
        while (itFields.hasNext()) {
            Map.Entry<String, JsonNode> field = itFields.next();
            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();
            String fieldType = null;
            if (fieldValue.isNumber()) {
                fieldType = "number";
            } else if (fieldValue.isTextual()) {
                fieldType = "string";
            } else {
                LOGGER.debug("Not handled copied message field type to accumulate metrics: " + fieldValue.getNodeType());
            }
            if (fieldType != null) {
                updateMetrics(destinationMetrics, fieldName, fieldType, fieldValue);
            }
        }
        destinationMetrics.compute("count", (k,v) -> (v==null)?1:v+1);
    }

    public void updateMetrics(Map<String, Double> metrics, String fieldName, String fieldType, JsonNode value) {
        // TODO handle more types
        if (fieldType.equals("number")) {
            // count
            metrics.compute(fieldName+".count", (k, v) -> (v==null)?1:v+1);
            // sum
            metrics.compute(fieldName+".sum", (k, v) -> (v==null)?1:v+value.numberValue().doubleValue()); // TODO... doubleValue always :-/
        } else if (fieldType.equals("string")) {
            // count
            metrics.compute(fieldName+".count", (k, v) -> (v==null)?1:v+1);
            // total length
            metrics.compute(fieldName+".sum", (k, v) -> (v==null)?1:v+value.textValue().length());
            // distinct 
            // TODO use CMS?
        }
    }

    public void finishCopy() {
        // sending sourceDS, sourceSC, destinationDS
        try {
            this.observationsAPI.reportDataSource(sourceDS);
            this.observationsAPI.reportSchema(sourceSC);
            this.observationsAPI.reportDataSource(destinationDS);
            this.observationsAPI.reportSchema(destinationSC);
        } catch (ApiException e) {
            LOGGER.error("Cannot report datasource and schema", e);
        }

        // compute final lineage using final schemas
        Set<String> sourceFieldNames = sourceSC.getPk().getFields().stream().map(e -> e.getName()).collect(Collectors.toSet());
        SchemaRef sourceSCRef = new SchemaRef().byPK(this.sourceSC.getPk());
        SchemaRef destinationSCRef = new SchemaRef().byPK(this.destinationSC.getPk());
        // TODO: Mapper could be introspected to find the connection between names (normalization, ...)
        // So this best effort only link fields of same names
        Map<String, List<String>> bestEffortMapping = new HashMap<>();
        for (FieldDef fd : destinationSC.getPk().getFields()) {
            if (sourceFieldNames.contains(fd.getName())) {
                bestEffortMapping.put(fd.getName(), new ArrayList<>(List.of(fd.getName())));
            }
        }
        if (bestEffortMapping.isEmpty()) { bestEffortMapping.put("fake", new ArrayList<>(List.of("fake"))); } // ensure there is something... 
        lineage = new ProcessLineage().name("Skip")
                                        .pk(new ProcessLineagePK()
                                                .processRef(new ProcessRef().byPK(this.process.getPk()))
                                                .dataFlow(new ArrayList<>(List.of(new SchemaLineageDependencyDef()
                                                                                    .fromSchemaRef(sourceSCRef)
                                                                                    .toSchemaRef(destinationSCRef)
                                                                                    .columnDataDependencies(bestEffortMapping)))));
        lineageRun = new LineageRun().pk(new LineageRunPK()
                                            .timestamp(System.currentTimeMillis())
                                            .processRunRef(new ProcessRunRef().byPK(this.processRun.getPk()))
                                            .lineageRef(new ProcessLineageRef().byPK(this.lineage.getPk())));
        // send lineage and run
        try {
            this.observationsAPI.reportProcessLineage(lineage);
            this.observationsAPI.reportLineageRun(lineageRun);
        } catch (ApiException e) {
            LOGGER.error("Cannot report lineage, lineageRun", e);
        }

        // Compile the stats and send
        DataStats sourceDSMetrics = new DataStats().pk(new DataStatsPK().schemaRef(new SchemaRef().byPK(sourceSC.getPk()))
                                                                        .lineageRunRef(new LineageRunRef().byPK(lineageRun.getPk()))
                                                    ).stats(sourceMetrics.entrySet().stream().collect(Collectors.toMap(
                                                        e->e.getKey(), e->BigDecimal.valueOf(e.getValue())
                                                    )));

        DataStats destinationDSMetrics = new DataStats().pk(new DataStatsPK().schemaRef(new SchemaRef().byPK(destinationSC.getPk()))
                                                                            .lineageRunRef(new LineageRunRef().byPK(lineageRun.getPk())))
                                                    .stats(destinationMetrics.entrySet().stream().collect(Collectors.toMap(
                                                        e->e.getKey(), e->BigDecimal.valueOf(e.getValue())
                                                    )));
        
        // sending stats
        try {
            this.observationsAPI.reportDataStats(sourceDSMetrics);
            this.observationsAPI.reportDataStats(destinationDSMetrics);
        } catch (ApiException e) {
            LOGGER.error("Cannot report datastats", e);
        }

        // notify agent is done => clear cache in Factory 
        //   => should be handled by Factory, registering itself to act on "termination"
        KensuAgentFactory.terminate(this);
    }

    private static PhysicalLocation UNKNOWN_PL = new PhysicalLocation()
                                                .name("Unknown")
                                                .lat(0.12341234)
                                                .lon(0.12341234)
                                                .pk(new PhysicalLocationPK().country("Unknown").city("Unknown"));
    private static PhysicalLocationRef UNKNOWN_PL_REF = new PhysicalLocationRef().byPK(UNKNOWN_PL.getPk());
}
