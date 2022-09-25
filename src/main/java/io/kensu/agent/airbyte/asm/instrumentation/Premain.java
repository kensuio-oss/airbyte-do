package io.kensu.agent.airbyte.asm.instrumentation;


import java.lang.instrument.Instrumentation;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import org.objectweb.asm.*;
import org.objectweb.asm.util.*;

import io.airbyte.workers.general.DefaultReplicationWorker;

public class Premain {
    static boolean printClassByteCode = false;
    public static void premain(String agentArgs, Instrumentation inst) throws Exception {
        inst.addTransformer(new ClassFileTransformer() {
            public byte[] transform(ClassLoader l, String name, Class c, ProtectionDomain d, byte[] b) throws IllegalClassFormatException {
                // The class to be injected
                if(name.equals(Type.getType(DefaultReplicationWorker.class).getInternalName())) {
                    // Read the class bytecode
                    ClassReader cr = new ClassReader(b);
                    // Write the injected class bytecode
                    ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_FRAMES);
                    // Check if class byte code mush be printed (sout)
                    ClassVisitor cv;
                    if (printClassByteCode) {
                        TraceClassVisitor tcv = new TraceClassVisitor(cw, new java.io.PrintWriter(System.out));
                        // Create the class adapter that injects code
                        cv = new AirbyteDefaultReplicationWorkerAdapter(tcv);
                    } else {
                        // Create the class adapter that injects code
                        cv = new AirbyteDefaultReplicationWorkerAdapter(cw);

                    }
                    // Run the injection
                    //cr.accept(cv, 0);
                    cr.accept(cv, 0);
                    // Return the injected code to replace the original class
                    return cw.toByteArray();
                }
                return b;
        } });
    }

}