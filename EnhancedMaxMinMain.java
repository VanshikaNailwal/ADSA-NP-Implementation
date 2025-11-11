package org.cloudbus.cloudsim.examples.research; 
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim; 
import org.cloudbus.cloudsim.provisioners.*;
import java.text.DecimalFormat;
import java.util.*;



public class EnhancedMaxMinMain {

    public static void main(String[] args) {
        System.out.println("Starting Enhanced Max-Min Scheduling Simulation...");

        try {
            // 1. Initialize CloudSim
            int numUser = 1;
            Calendar calendar = Calendar.getInstance();
            boolean traceFlag = false;
            CloudSim.init(numUser, calendar, traceFlag);

            // 2. Create Datacenter
            Datacenter datacenter0 = createDatacenter("Datacenter_0");

            // 3. Create Broker
            EnhancedMaxMinBroker broker = new EnhancedMaxMinBroker("EnhancedMaxMin_Broker");
            int brokerId = broker.getId();

            // 4. Create VMs (same as other algorithms)
            List<Vm> vmList = new ArrayList<>();
            int[] mips = {1000, 2500, 4000};
            for (int i = 0; i < 3; i++) {
                Vm vm = new Vm(i, brokerId, mips[i], 1, 2048, 1000, 10000, "Xen",
                        new CloudletSchedulerTimeShared());
                vmList.add(vm);
            }
            broker.submitVmList(vmList);

            // 5. Create Cloudlets (same as other algorithms)
            List<Cloudlet> cloudletList = new ArrayList<>();
            UtilizationModel utilizationModel = new UtilizationModelFull();
            Random rand = new Random(42);
            for (int i = 0; i < 20; i++) {
                int length = 3000 + rand.nextInt(9000); // 3000–12000
                Cloudlet cloudlet = new Cloudlet(i, length, 1, 300, 300,
                        utilizationModel, utilizationModel, utilizationModel);
                cloudlet.setUserId(brokerId);
                cloudletList.add(cloudlet);
            }
            broker.submitCloudletList(cloudletList);

            // 6. Start Simulation
            CloudSim.startSimulation();
            CloudSim.stopSimulation();

            // 7. Print results in same format
            printCloudletListWithMetrics(broker.getCloudletReceivedList());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Datacenter createDatacenter(String name) throws Exception {
        List<Pe> peList = new ArrayList<>();
        peList.add(new Pe(0, new PeProvisionerSimple(5000)));
        peList.add(new Pe(1, new PeProvisionerSimple(5000)));

        List<Host> hostList = new ArrayList<>();
        hostList.add(new Host(0,
                new RamProvisionerSimple(8192),
                new BwProvisionerSimple(10000),
                1000000, peList,
                new VmSchedulerTimeShared(peList)
        ));

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                "x86", "Linux", "Xen", hostList, 10.0, 3.0, 0.05, 0.001, 0.0
        );

        return new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList),
                new LinkedList<>(), 0);
    }

    private static void printCloudletListWithMetrics(List<Cloudlet> list) {
        System.out.println("\n========== OUTPUT ==========");
        System.out.printf("%-10s %-10s %-10s %-15s %-15s %-10s%n",
                "Cloudlet", "Status", "VM", "Start Time", "Finish Time", "Exec Time");

        double makespan = 0, totalExec = 0, totalWait = 0;
        Map<Integer, Double> vmWork = new HashMap<>();

        for (Cloudlet c : list) {
            System.out.printf("%-10d %-10s %-10d %-15.2f %-15.2f %-10.2f%n",
                    c.getCloudletId(),
                    (c.getStatus() == Cloudlet.SUCCESS ? "SUCCESS" : "FAILED"),
                    c.getVmId(),
                    c.getExecStartTime(),
                    c.getFinishTime(),
                    c.getActualCPUTime());

            makespan = Math.max(makespan, c.getFinishTime());
            totalExec += c.getActualCPUTime();
            totalWait += (c.getExecStartTime() - c.getSubmissionTime());

            vmWork.put(c.getVmId(), vmWork.getOrDefault(c.getVmId(), 0.0) + c.getActualCPUTime());
        }

        double avgExec = totalExec / list.size();
        double avgWait = totalWait / list.size();
        double throughput = list.size() / makespan;

        // Load Balance: Std Dev of VM workload
        double meanWork = vmWork.values().stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double variance = vmWork.values().stream().mapToDouble(w -> Math.pow(w - meanWork, 2)).sum() / vmWork.size();
        double stdDev = Math.sqrt(variance);

        System.out.println("\n========== ENHANCED MAX-MIN METRICS ==========");
        System.out.printf("%-30s : %.2f sec%n", "Makespan", makespan);
        System.out.printf("%-30s : %.2f sec%n", "Average Execution Time", avgExec);
        System.out.printf("%-30s : %.2f sec%n", "Average Waiting Time", avgWait);
        System.out.printf("%-30s : %.2f Cloudlets/sec%n", "Throughput", throughput);
        System.out.printf("%-30s : %.4f%n", "VM Load Std Dev (Load Balance)", stdDev);
        System.out.println("=============================================");
    }
}
