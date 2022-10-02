package io.kensu.agent.airbyte.asm.instrumentation;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;

public class ClassWriterAirbyte extends ClassWriter {

    public ClassWriterAirbyte(ClassReader classReader, int flags) {
        super(classReader, flags);
    }

    @Override
    protected String getCommonSuperClass(String type1, String type2) {
        System.out.println("-------------------------------");
        System.out.println("Type1 : " + type1);
        System.out.println("Type2 : " + type2);
        
        if (type1.equals("java/lang/Exception") && type2.equals("io/airbyte/workers/internal/AirbyteDestination")) {
            System.out.println("Returning object");
            return "java/lang/Object";
        } else if (type1.equals("java/lang/Object") && type2.equals("io/airbyte/workers/internal/AirbyteDestination")) {
            System.out.println("Returning object");
            return "java/lang/Object";
        }
        
        return super.getCommonSuperClass(type1, type2);
    }
        
}