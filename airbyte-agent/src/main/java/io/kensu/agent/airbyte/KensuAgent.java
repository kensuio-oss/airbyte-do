package io.kensu.agent.airbyte;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import io.airbyte.config.StandardSyncInput;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
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

    private String projectName;
    public Project project;
    public Process process;
    public ProcessRun processRun;
    private String userName;
    public User launchingUser;
    private String maintainerName;
    public User maintainerUser;
    private String codeBaseLocation;
    public CodeBase codeBase;
    private String codeVersionVersion;
    public CodeVersion codeVersion;
    private String environment;

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

        this.projectName = configuration.getProperty("kensu.project.name");
        if (this.projectName == null || this.projectName.trim().length() == 0) {
            LOGGER.warn("No project name configured, fallback on default");
            //TODO Provided By Airbyte?
            this.projectName = "Airbyte";
        }
        this.userName = configuration.getProperty("kensu.user.name");
        if (this.userName == null || this.userName.trim().length() == 0) {
            LOGGER.warn("No user name configured, fallback on default if no var env");
            //TODO Provided By Airbyte?
            this.userName = System.getenv().getOrDefault("USER", "Airbyte");
        }
        this.codeBaseLocation = configuration.getProperty("kensu.codebase.location");
        if (this.codeBaseLocation == null || this.codeBaseLocation.trim().length() == 0) {
            LOGGER.warn("No code base location configured, fallback on default");
            //TODO Provided By Airbyte?
            this.codeBaseLocation = "AirbyteCodeBase";
        }
        this.codeVersionVersion = configuration.getProperty("kensu.codeversion.version"); 
        if (this.codeVersionVersion == null || this.codeVersionVersion.trim().length() == 0) {
            LOGGER.warn("No code version configured, fallback on default");
            // TODO could be a version of a connection?
            this.codeVersionVersion = "AirbyteVersion";
        }
        this.maintainerName = configuration.getProperty("kensu.maintainer.name");
        if (this.maintainerName == null || this.maintainerName.trim().length() == 0) {
            LOGGER.warn("No maintainer name configured, fallback on default on user name");
            //TODO Provided By Airbyte?
            this.maintainerName = this.userName;
        }
        this.environment = configuration.getProperty("kensu.environment");
        if (this.environment == null || this.environment.trim().length() == 0) {
            LOGGER.warn("No environment configured, fallback on default");
            //TODO Provided By Airbyte?
            this.environment = "lab";
        }

        if (sourceImage != null && destinationImage != null) {
            // We make the choice that each sync is a process, not the Airbyte Server
            // A process is currently representing the link between a Source type (docker) and destination type (docker)
            // TODO => should it be coming from the syncInput.catalog.streams?
            // TODO should be using more information about the datasources!
            this.process = new Process().pk(new ProcessPK().qualifiedName("Airbyte connection:" + sourceImage + "->"+ destinationImage)); 

            this.project = new Project().pk(new ProjectPK().name(this.projectName));
            this.launchingUser = new User().pk(new UserPK().name(this.userName));
            this.codeBase = new CodeBase().pk(new CodeBasePK().location(this.codeBaseLocation));
            this.maintainerUser = new User().pk(new UserPK().name(this.maintainerName));
            this.codeVersion = new CodeVersion().pk(new CodeVersionPK().version(this.codeVersionVersion)
                                                                        .codebaseRef(new CodeBaseRef().byPK(this.codeBase.getPk())))
                                                .maintainersRefs(new ArrayList<>(List.of(new UserRef().byPK(this.maintainerUser.getPk()))));
            this.processRun = new ProcessRun().projectsRefs(new ArrayList<>(List.of(new ProjectRef().byPK(this.project.getPk())))) 
                                                .launchedByUserRef(new UserRef().byPK(launchingUser.getPk()))
                                                .executedCodeVersionRef(new CodeVersionRef().byPK(this.codeVersion.getPk()))
                                                .environment(this.environment)
                                                .pk(new ProcessRunPK().qualifiedName(process.getPk().getQualifiedName())
                                                                        .processRef(new ProcessRef().byPK(process.getPk()))); 
            // sending observations   
            try {
                observationsAPI.reportProject(project);
                observationsAPI.reportProcess(process);
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

    // TODO could be filled reading the specs YAML
    public List<KensuProcessor> sources = List.of(
        new Sources.AirbyteSourceFile()
    );
    public List<KensuProcessor> destinations = List.of(
        new Destinations.AirbyteDestinationCSV(),
        new Destinations.AirbyteDestinationBigQuery()
    );

    public void init(StandardSyncInput syncInput) {
        if (sourceImage == null) {
            LOGGER.error("Cannot proceed as source image name is unknown");
            return;
        } else {
            Optional<KensuProcessor> sourceProcessor = sources.stream().filter(s->s.matches(sourceImage)).findFirst();
            if (sourceProcessor.isPresent()) {
                sourceProcessor.get().process(syncInput, this);
            } else {
                LOGGER.error("Cannot handle source image: " + sourceImage);
            }
        }
        if (destinationImage == null ) {
            LOGGER.error("Cannot proceed as destination image name is unknown");
            return;
        } else {
            Optional<KensuProcessor> destinationProcessor = destinations.stream().filter(e->e.matches(destinationImage)).findFirst();
            if (destinationProcessor.isPresent()) {
                destinationProcessor.get().process(syncInput, this);
            } else {
                LOGGER.error("Cannot handle destination image: " + destinationImage);
            }
        }
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
            // TODO Date, ...
            if (fieldValue.isNumber()) {
                fieldType = "number";
            } else if (fieldValue.isTextual()) {
                fieldType = "string";
            } else if (fieldValue.isBoolean()) {
                fieldType = "boolean";
            } else if (fieldValue.isNull()) {
                fieldType = null;
                LOGGER.debug("TODO handle null values for field: " + fieldName);
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
        if (value != null) {
            if (value.isNull()) { // probably not gonna be here
                value = null;
            }
        } 
        if (value == null) {
            // count.null
            metrics.compute(fieldName+".nullrows", (k, v) -> (v==null)?1:v+1);
        } else {
            // count
            metrics.compute(fieldName+".count", (k, v) -> (v==null)?1:v+1);
            if (fieldType.equals("number")) {
                final double doubleValue = value.numberValue().doubleValue();
                // sum
                metrics.compute(fieldName+".sum", (k, v) -> ((v==null)?0:v)+doubleValue); // TODO... doubleValue always :-/
            } else if (fieldType.equals("string")) {
                final String stringValue = value.textValue();
                // total length
                metrics.compute(fieldName+".sum", (k, v) -> ((v==null)?0:v)+stringValue.length());
                // distinct 
                // TODO use CMS?
                // categories hist
                // TODO 
            } else if (fieldType.equals("boolean")) {
                final boolean boolValue = value.booleanValue();
                // true/false counts
                metrics.compute(fieldName+(boolValue?".true":".false"), (k, v) -> (v==null)?1:v+1);
            }
        }
    }

    public void finishCopy() {
        // sending sourceDS, sourceSC, destinationDS
        List<String> missingObjects = new ArrayList<>();
        try {
            if (sourceDS != null) {
                this.observationsAPI.reportDataSource(sourceDS);
                if (sourceSC != null)
                    this.observationsAPI.reportSchema(sourceSC);
                else 
                    missingObjects.add("Source Schema");
            } else 
                missingObjects.add("Source DataSource");
            if (destinationDS != null) {
                this.observationsAPI.reportDataSource(destinationDS);
                if (destinationSC != null)
                    this.observationsAPI.reportSchema(destinationSC);
                else 
                    missingObjects.add("Destination Schema");
            } else 
                missingObjects.add("Destination DataSource");
        } catch (ApiException e) {
            LOGGER.error("Cannot report datasource and schema", e);
        }

        if (!missingObjects.isEmpty()) {
            LOGGER.warn("Skipping the `finishCopy` without computing lineage as some objects are missing: " + Arrays.toString(missingObjects.toArray()));
            // WARN... early stop here
            KensuAgentFactory.terminate(this);
            return;
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

    public static PhysicalLocation UNKNOWN_PL = new PhysicalLocation()
                                                .name("Unknown")
                                                .lat(0.12341234)
                                                .lon(0.12341234)
                                                .pk(new PhysicalLocationPK().country("Unknown").city("Unknown"));
    public static PhysicalLocationRef UNKNOWN_PL_REF = new PhysicalLocationRef().byPK(UNKNOWN_PL.getPk());
}
