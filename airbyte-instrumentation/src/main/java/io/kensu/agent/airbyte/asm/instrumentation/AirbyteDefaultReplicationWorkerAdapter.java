package io.kensu.agent.airbyte.asm.instrumentation;

import java.util.stream.Stream;

import javax.naming.NamingSecurityException;
import javax.xml.transform.Source;

import org.objectweb.asm.*;
import static org.objectweb.asm.Opcodes.*;
import org.objectweb.asm.commons.AdviceAdapter;
import org.objectweb.asm.commons.LocalVariablesSorter;
import org.objectweb.asm.commons.Method;

public class AirbyteDefaultReplicationWorkerAdapter extends ClassVisitor {

    public String staticLambdaFunctionName;

    /**
     * getReplicationRunnable returns a Runnable created as a `lambda`: `() -> {}`
     * that implements the `run` method of Runnable
     * This lambda is turned into a static method of the owner class:
     * `DefaultReplicationWorker`
     * Because everything happens in this `run` method, we need to update this one,
     * and not getReplicationRunnable
     * We therefore need its name, which we collect while visiting
     * getReplicationRunnable, which itself call visitInvokeDynamicInsn
     * visitInvokeDynamicInsn has a bootstrap arguments which contains the Handler
     * (the callsite that must be executed) => this is what we need
     */

    public AirbyteDefaultReplicationWorkerAdapter(ClassVisitor cv) {
        super(ASM9, cv);
    }

    // @Override
    public MethodVisitor visitMethod​(int access, String name, String descriptor, String signature,
            String[] exceptions) {
        MethodVisitor mv = cv.visitMethod(access, name, descriptor, signature, exceptions);
        if (name.equals("getReplicationRunnable")) {
            System.out.println("Got getReplicationRunnable!");
            mv = new MethodVisitors.MethodVisitorReturnRunnable(mv, this);
        } else if (name.equals(this.staticLambdaFunctionName)) {
            System.out.println("Got the lambda: " + this.staticLambdaFunctionName + "!");
            MethodVisitors.MethodVisitorRunnableRunLambda mvr = new MethodVisitors.MethodVisitorRunnableRunLambda(descriptor, mv);
            LocalVariablesSorter lvs = new LocalVariablesSorter(access, descriptor, mvr);
            mvr.lvs = lvs;
            mv = lvs;
        }
        return mv;
    }

    public static interface MethodVisitors {
        public static class MethodVisitorReturnRunnable extends MethodVisitor {
            public AirbyteDefaultReplicationWorkerAdapter ca;

            public MethodVisitorReturnRunnable(MethodVisitor mv, AirbyteDefaultReplicationWorkerAdapter ca) {
                super(ASM9, mv);
                this.ca = ca;
            }

            @Override
            public void visitInvokeDynamicInsn​(String name, String descriptor, Handle bootstrapMethodHandle,
                    Object... bootstrapMethodArguments) {
                Object[] args = bootstrapMethodArguments;
                if (name.equals("run") && descriptor.endsWith("Ljava/lang/Runnable;")) {
                    // get the handle to the code that must be executed => the Runnable.run lambda
                    String staticLambdaFunctionName = Stream.of(args)
                            .filter(a -> a instanceof Handle
                                    && ((Handle) a).getOwner().equals(Constants.DefaultReplicationWorkerInternalName))
                            .map(a -> ((Handle) a).getName())
                            .findFirst()
                            .orElse(null);
                    if (staticLambdaFunctionName != null) {
                        this.ca.staticLambdaFunctionName = staticLambdaFunctionName;
                    }
                }
                this.mv.visitInvokeDynamicInsn​(name, descriptor, bootstrapMethodHandle, bootstrapMethodArguments);
            }
        }

        public static class MethodVisitorRunnableRunLambda extends MethodVisitor /* to add local variables */ {
            public int argOfTypeSourceIndex = -1;
            public int argOfTypeDestinationIndex = -1;
            public LocalVariablesSorter lvs;
            private int tracker;
            public Label initCodeLabel = new Label();
            public Label addedCodeLabel = new Label();
            public Label afterAddedCodeLabel = new Label();

            public MethodVisitorRunnableRunLambda(String desc, MethodVisitor mv) {
                super(ASM9, mv);
                System.out.println("Create MethodVisitorRunnableRunLambda");
                Method thisMethod = new Method("whocares", desc);
                int index = 0;
                for (Type at : thisMethod.getArgumentTypes()) {
                    String atDesc = at.getDescriptor();
                    if (atDesc.equals(Constants.AirbyteSourceDescriptor)) {
                        // Retain source index on the stack for future usage
                        this.argOfTypeSourceIndex = index;
                        System.out.println("(By type) Index of the source is: " + this.argOfTypeSourceIndex);
                    } else if (atDesc.equals(Constants.AirbyteDestinationDescriptor)) {
                        // Retain destination index on the stack for future usage
                        this.argOfTypeDestinationIndex = index;
                        System.out.println("(By type) Index of the destination is: " + this.argOfTypeDestinationIndex);
                    }
                    index++;
                }
            }

            @Override
            public void visitCode() {
                System.out.println("visitCode");
                mv.visitCode();

                addCode();

                // jumping to the added code to init the tracker using the local var indices
                // captured before
                //mv.visitJumpInsn(GOTO, addedCodeLabel);

                // this for the added code to return here and execute the initial code
                //mv.visitLabel(initCodeLabel);
            }

            private void addCode() {
                System.out.println("addCode");

                // Get the tracker Factory
                mv.visitMethodInsn(INVOKESTATIC, Constants.KensuAgentFactoryInternalName, "getInstance",
                        "()" + Constants.KensuAgentFactoryDescriptor, false);

                // Use the factory to create tracker using the input
                mv.visitMethodInsn(INVOKEVIRTUAL, Constants.KensuAgentFactoryInternalName, "create",
                        "()" + Constants.KensuAgentDescriptor, false);
                // Create a new local slot for the tracker
                tracker = lvs.newLocal(Type.getType(Constants.KensuAgentDescriptor));
                // Store the tracker
                mv.visitVarInsn(ASTORE, tracker);
                // Load the tracker
                mv.visitVarInsn(ALOAD, tracker);
                // Load source
                mv.visitVarInsn(ALOAD, argOfTypeSourceIndex);
                // Load the output
                mv.visitVarInsn(ALOAD, argOfTypeDestinationIndex);
                // Use the trackers's method (to) with the output
                mv.visitMethodInsn(INVOKEVIRTUAL, Constants.KensuAgentInternalName, "observerReplication",
                        "(" + Constants.AirbyteSourceDescriptor + Constants.AirbyteDestinationDescriptor + ")V", false);
                
                System.out.println("end addCode");
            }

            @Override
            public void visitMaxs(int maxStack, int maxLocals) {
                System.out.println("visitMaxs");
                // we don't compute them, because we let the class writer do it
                mv.visitMaxs(maxStack, maxLocals);
            }
        }
    }
}