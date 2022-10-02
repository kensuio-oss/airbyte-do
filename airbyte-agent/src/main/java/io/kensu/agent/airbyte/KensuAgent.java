package io.kensu.agent.airbyte;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.math.BigDecimal;

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

    public ManageKensuDamEntitiesApi api;

    public KensuAgent(AirbyteSource source, AirbyteDestination destination, AirbyteMapper mapper,
            MessageTracker messageTracker, RecordSchemaValidator recordSchemaValidator,
            WorkerMetricReporter metricReporter) {

        String apiHost = System.getenv("DAM_INGESTION_URL"); // TODO => CONF
        String authToken = System.getenv("AUTH_TOKEN"); // TODO => how to have a token per process?

        ApiClient apiClient = null;
        if (System.getenv("KENSU_ONLINE") != null) {
            // Online
            apiClient = new ApiClient()
                    .setBasePath(apiHost)
                    .addDefaultHeader("X-Auth-Token", authToken);
        } else {
            // Offline client
            apiClient = new OfflineFileApiClient();
        }
        this.api = new ManageKensuDamEntitiesApi(apiClient);        

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
            this.process = new Process().pk(new ProcessPK().qualifiedName("Airbyte:" + sourceImage + "->"+ destinationImage));
            this.project = new Project().pk(new ProjectPK().name("Provided By Airbyte?")); //TODO
            this.launchingUser = new User().pk(new UserPK().name("Provided by Airbyte?")); //TODO
            this.codeBase = new CodeBase().pk(new CodeBasePK().location("Provided by Airbyte?"));  //TODO
            this.maintainerUser = new User().pk(new UserPK().name("Provided by Airbyte?")); //TODO
            this.codeVersion = new CodeVersion().pk(new CodeVersionPK().version("Provided by Airbyte?")
                                                                        .codebaseRef(new CodeBaseRef().byPK(this.codeBase.getPk())))
                                                .maintainersRefs(java.util.List.of(new UserRef().byPK(this.maintainerUser.getPk())));
            this.processRun = new ProcessRun().projectsRefs(java.util.List.of(new ProjectRef().byPK(this.project.getPk()))) 
                                                .launchedByUserRef(new UserRef().byPK(launchingUser.getPk()))
                                                .executedCodeVersionRef(new CodeVersionRef().byPK(this.codeVersion.getPk()))
                                                .environment("Provided by Airbyte?"); //TODO      
            // TODO send      
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

    public void init(StandardSyncInput syncInput) {
        // e.g., "airbyte/source-file:0.2.20"
        if (sourceImage == null) {
            LOGGER.error("Cannot proceed as source image name is unknown");
            return;
        } else if (sourceImage.startsWith("airbyte/source-file")) { // source: https CSV
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
                SchemaPK pk = new SchemaPK();
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
        // e.g., "airbyte/destination-csv:0.2.10"
        if (destinationImage == null ) {
            LOGGER.error("Cannot proceed as destination image name is unknown");
            return;
        } else if (destinationImage.startsWith("airbyte/destination-csv")) { // destination: local CSV
            // configured destination for the sync
            JsonNode destinationConfiguration = syncInput.getDestinationConfiguration();
            //"/tmp"
            String destinationPath = destinationConfiguration.get("destination_path").textValue();
            destinationDS = new DataSource()
                                .name("TODO?") //TODO
                                .format("csv")
                                .pk(new DataSourcePK()
                                    .location(destinationPath) //TODO => destination_path this is the folder, not the final file
                                    .physicalLocationRef(UNKNOWN_PL_REF));
            destinationSC = new Schema().name(destinationDS.getName()).pk(new SchemaPK());
        }

        // INFO => the destination of the destination is not known -> so we copy the source
        // TODO send sourceDS, sourceSC, destinationDS
    }

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
                        LOGGER.warn("Not handled mapped message field type: " + fieldValue.getNodeType());
                    }
                    if (fieldType != null) {
                        destinationSC.getPk().addFieldsItem(new FieldDef().name(fieldName).fieldType(fieldType).nullable(true));
                    }
                }
                // TODO need to add "unknown" type for fields which for all message have only null values! 
                //      As it won't be added in the schema then
            }
            Set<String> sourceFieldNames = sourceSC.getPk().getFields().stream().map(e -> e.getName()).collect(Collectors.toSet());
            SchemaRef sourceSCRef = new SchemaRef().byPK(this.sourceSC.getPk());
            SchemaRef destinationSCRef = new SchemaRef().byPK(this.destinationSC.getPk());
            // INFO: Mapper is a black box... we can give it some extra power to also log its mapping, and retrieve it here, or in a span
            // So this best effort only link fields of same names
            Map<String, List<String>> bestEffortMapping = new HashMap<>();
            for (FieldDef fd : destinationSC.getPk().getFields()) {
                if (sourceFieldNames.contains(fd.getName())) {
                    bestEffortMapping.put(fd.getName(), List.of(fd.getName()));
                }
            }
            if (bestEffortMapping.isEmpty()) { bestEffortMapping.put("fake", List.of("fake")); } // ensure there is something... 
            lineage = new ProcessLineage().name("Skip")
                                            .pk(new ProcessLineagePK()
                                                    .dataFlow(List.of(new SchemaLineageDependencyDef()
                                                                            .fromSchemaRef(sourceSCRef)
                                                                            .toSchemaRef(destinationSCRef)
                                                                            .columnDataDependencies(bestEffortMapping))));
            lineageRun = new LineageRun().pk(new LineageRunPK()
                                                .processRunRef(new ProcessRunRef().byPK(this.processRun.getPk()))
                                                .lineageRef(new ProcessLineageRef().byPK(this.lineage.getPk())));
            // TODO send lineage and run
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
                    LOGGER.warn("Not handled read message field type to accumulate metrics: " + fieldValue.getNodeType());
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
                LOGGER.warn("Not handled copied message field type to accumulate metrics: " + fieldValue.getNodeType());
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
            // distinct?
            // TODO
        }
    }

    public void finishCopy() {
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
        
        // TODO send

        // TODO notify agent is done => clear cache in Factory
    }

    private static PhysicalLocation UNKNOWN_PL = new PhysicalLocation()
                                                .name("Unknown")
                                                .lat(0.12341234)
                                                .lon(0.12341234)
                                                .pk(new PhysicalLocationPK().country("Unknown").city("Unknown"));
    private static PhysicalLocationRef UNKNOWN_PL_REF = new PhysicalLocationRef().byPK(UNKNOWN_PL.getPk());
}
