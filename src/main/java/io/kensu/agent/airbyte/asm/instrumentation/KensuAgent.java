package io.kensu.agent.airbyte.asm.instrumentation;

import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteSource;

public class KensuAgent {
    public void observerReplication(AirbyteSource source, AirbyteDestination destination) {
        System.out.println("Source:" + source);
        System.out.println("Destination:" + destination);
    }
}
