package io.kensu.agent.airbyte;

public class KensuAgentFactory {
 
    private static KensuAgentFactory INSTANCE = new KensuAgentFactory();

    public KensuAgentFactory() {
        // TODO load config
        System.out.println("Create KensuAgentFactory");
    }

    public static KensuAgentFactory getInstance() {
        System.out.println("Get KensuAgentFactory Instance");
        return INSTANCE;
    }
  
    public KensuAgent create() {
        System.out.println("KensuAgentFactory.create");
        return new KensuAgent();
    }
}
