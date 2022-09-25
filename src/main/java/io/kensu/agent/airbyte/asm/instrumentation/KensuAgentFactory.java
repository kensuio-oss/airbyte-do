package io.kensu.agent.airbyte.asm.instrumentation;

public class KensuAgentFactory {
 
    private static KensuAgentFactory INSTANCE = new KensuAgentFactory();

    public static KensuAgentFactory getInstance() {
        return INSTANCE;
    }
  
    public KensuAgent create() {
        return new KensuAgent();
    }
}
