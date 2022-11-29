package io.kensu.agent.airbyte;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import io.airbyte.commons.text.Names;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;

import io.kensu.dam.*;
import io.kensu.dam.model.*;

public interface Destinations {
    public static class AirbyteDestinationCSV implements KensuProcessor {
        private static final Logger LOGGER = LoggerFactory.getLogger(KensuAgent.class);

        @Override
        public boolean matches(String imageName) {
            return imageName != null && imageName.startsWith("airbyte/destination-csv");
        }

        @Override
        public void process(StandardSyncInput syncInput, KensuAgent agent) {
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
            agent.destinationDS = new DataSource()
                                .name(fileName)
                                .format("csv")
                                .pk(new DataSourcePK()
                                    .location(fileLocation) 
                                    .physicalLocationRef(agent.UNKNOWN_PL_REF));
            agent.destinationSC = new Schema().name(agent.destinationDS.getName())
                                                .pk(new SchemaPK().dataSourceRef(new DataSourceRef().byPK(agent.destinationDS.getPk())));  
        }

    }

    public static class AirbyteDestinationBigQuery implements KensuProcessor {
        private static final Logger LOGGER = LoggerFactory.getLogger(KensuAgent.class);

        @Override
        public boolean matches(String imageName) {
            return imageName != null && imageName.startsWith("airbyte/destination-bigquery");
        }

        @Override
        public void process(StandardSyncInput syncInput, KensuAgent agent) {
            // configured destination for the sync
            JsonNode destinationConfiguration = syncInput.getDestinationConfiguration();

            //"psyc..."
            String project_id = destinationConfiguration.get("project_id").textValue();

            //"dataobservability"
            String dataset_id = destinationConfiguration.get("dataset_id").textValue();

            // FIXME => always only 1?
            //"green ingestion"
            ConfiguredAirbyteStream stream = syncInput.getCatalog().getStreams().get(0);
            String streamName = stream.getStream().getName();
            // TODO => get the table name from Destination configuration (using ConfigRepository?)
            // TODO use "dataset_location" configuration
            
            String dsName = project_id + "." + dataset_id + "." + streamName;


            String location = "bigquery://"+dsName;
            agent.destinationDS = new DataSource()
                                .name(dsName)
                                .format("BigQuery")
                                .pk(new DataSourcePK()
                                    .location(location) 
                                    .physicalLocationRef(agent.UNKNOWN_PL_REF));
            agent.destinationSC = new Schema().name(agent.destinationDS.getName())
                                                .pk(new SchemaPK().dataSourceRef(new DataSourceRef().byPK(agent.destinationDS.getPk())));  
        }

    }
}
