package io.kensu.agent.airbyte;

import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteSource;

public class KensuAgent {
    
    public KensuAgent() {
        System.out.println("KensuAgent.new");
    }

    public void observerReplication(AirbyteSource source, AirbyteDestination destination) {
        System.out.println("Source:" + source);
        System.out.println("Destination:" + destination);
    }
}
