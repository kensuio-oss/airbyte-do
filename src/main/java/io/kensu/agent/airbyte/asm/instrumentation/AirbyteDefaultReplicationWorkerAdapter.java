package io.kensu.agent.airbyte.asm.instrumentation;

import java.util.stream.Stream;

import javax.naming.NamingSecurityException;
import javax.xml.transform.Source;

import org.objectweb.asm.*;
import static org.objectweb.asm.Opcodes.*;
import org.objectweb.asm.commons.LocalVariablesSorter;
import org.objectweb.asm.commons.AdviceAdapter;

import io.airbyte.workers.general.DefaultReplicationWorker;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteSource;

public class AirbyteDefaultReplicationWorkerAdapter extends ClassVisitor {

  public String staticLambdaFunctionName;

  /**
   * getReplicationRunnable returns a Runnable created as a `lambda`: `() -> {}` that implements the `run` method of Runnable
   * This lambda is turned into a static method of the owner class: `DefaultReplicationWorker`
   * Because everything happens in this `run` method, we need to update this one, and not getReplicationRunnable
   * We therefore need its name, which we collect while visiting getReplicationRunnable, which itself call visitInvokeDynamicInsn
   * visitInvokeDynamicInsn has a bootstrap arguments which contains the Handler (the callsite that must be executed) => this is what we need
   */

  public AirbyteDefaultReplicationWorkerAdapter(ClassVisitor cv) {
    super(ASM9, cv);
  }

  // @Override
  public MethodVisitor visitMethod​(int access, String name, String descriptor, String signature, String[] exceptions) {
    MethodVisitor mv = cv.visitMethod(access, name, descriptor, signature, exceptions);
    if (name.equals("getReplicationRunnable")) {
        System.out.println("Got getReplicationRunnable!");
        mv = new MethodVisitors.MethodVisitorReturnRunnable(mv, this);
    } else if (name.equals(this.staticLambdaFunctionName)) {
        System.out.println("Got the lambda: " + this.staticLambdaFunctionName + "!");
        mv = new MethodVisitors.MethodVisitorRunnableRunLambda(access, descriptor, mv);    
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
        public void visitInvokeDynamicInsn​(String name, String descriptor, Handle bootstrapMethodHandle, Object... bootstrapMethodArguments) {
            Object[] args = bootstrapMethodArguments;
            if (name.equals("run") && descriptor.endsWith("Ljava/lang/Runnable;")) {
                // get the handle to the code that must be executed => the Runnable.run lambda
                String staticLambdaFunctionName = Stream.of(args)
                        .filter(a -> a instanceof Handle && ((Handle) a).getOwner().equals(Type.getType(DefaultReplicationWorker.class).getInternalName()))
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

     public static class MethodVisitorRunnableRunLambda extends LocalVariablesSorter /* to add local variables */ {
         public int argOfTypeSourceIndex = -1;
         public int argOfTypeDestinationIndex = -1;
         private int tracker;
         public Label initCodeLabel = new Label();
         public Label addedCodeLabel = new Label();
         public Label afterAddedCodeLabel = new Label();

         public Type kensuAgentType = Type.getType(KensuAgent.class);
         public Type kensuAgentFactoryType = Type.getType(KensuAgentFactory.class);
         public Type airbyteSourceType = Type.getType(AirbyteSource.class);
         public Type airbyteDestinationType = Type.getType(AirbyteDestination.class);

         public MethodVisitorRunnableRunLambda(int access, String desc, MethodVisitor mv) {
             super(ASM9, access, desc, mv);
         }

         @Override
         public void visitLocalVariable​(String name, String descriptor, String signature, Label start, Label end, int index) {
             {
                 // Capture the indices of the source and destination variables provided to the run closure   
                 if (argOfTypeSourceIndex == -1) {
                     // Catch the local variable named "source" of type "AirbyteSource"
                     if (name.equals("source") && descriptor.equals(airbyteSourceType.getDescriptor())) {
                         // Retain its index on the stack for future usage
                         this.argOfTypeSourceIndex = index;
                         System.out.println("Index of the source is: " + this.argOfTypeSourceIndex);
                     }
                 } else if (argOfTypeDestinationIndex == -1) {
                     // Catch the local variable named "destination" of type "AirbyteDestination"
                     if (name.equals("destination") && descriptor.equals(airbyteDestinationType.getDescriptor())) {
                         // Retain its index on the stack for future usage
                         this.argOfTypeDestinationIndex = index;
                         System.out.println("Index of the destination is: " + this.argOfTypeDestinationIndex);
                     }
                 }
             }
             this.mv.visitLocalVariable​(name, descriptor, signature, start, end, index);
         }

         @Override
         public void visitCode() {
             mv.visitCode();

             // jumping to the added code to init the tracker using the local var indices captured before
             mv.visitJumpInsn(GOTO, addedCodeLabel);

             // this for the added code to return here and execute the initial code
             mv.visitLabel(initCodeLabel);
         }

         private void addCode() {
             // Get the tracker Factory
             mv.visitMethodInsn(INVOKESTATIC, kensuAgentFactoryType.getInternalName(), "getInstance", "()"+kensuAgentFactoryType.getDescriptor(), false);
             // Create a new local slot for the tracker
             tracker = newLocal(Type.getType(kensuAgentType.getDescriptor()));
             // Load input
             mv.visitVarInsn(ALOAD, argOfTypeSourceIndex);
             // Use the factory to create tracker using the input
             mv.visitMethodInsn(INVOKEVIRTUAL, kensuAgentFactoryType.getInternalName(), "create", "()"+kensuAgentType.getDescriptor(), false);
             // Store the tracker
             mv.visitVarInsn(ASTORE, tracker);
             // Load the tracker
             mv.visitVarInsn(ALOAD, tracker);
             // Load the output
             mv.visitVarInsn(ALOAD, argOfTypeDestinationIndex);
             // Use the trackers's method (to) with the output
             mv.visitMethodInsn(INVOKEVIRTUAL, kensuAgentType.getInternalName(), "observerReplication", 
             "("+airbyteSourceType.getDescriptor()+airbyteDestinationType.getDescriptor()+")V", false);
         }

         @Override
         public void visitMaxs(int maxStack, int maxLocals) {
             // to avoid the code to execute the added code again, we jump after it
             mv.visitJumpInsn(GOTO, afterAddedCodeLabel);

             // this is the start of the added code, the method will jump directly here before going back to normal flow
             mv.visitLabel(addedCodeLabel);

             // Add the code
             addCode();
             
             // now that the added code is executed, we can go back to the original code and execute it
             mv.visitJumpInsn(GOTO, initCodeLabel);

             // this allows the added code to be skipped (see above)
             mv.visitLabel(afterAddedCodeLabel);
             
             // we don't compute them, because we let the class writer do it
             mv.visitMaxs(maxStack, maxLocals);
         }
     }
  }
}