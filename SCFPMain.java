package org.cloudbus.cloudsim.examples.research;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import java.util.*;
import org.cloudbus.cloudsim.core.CloudSim;

/**
 * SCFP Simulation: Shortest Cloudlet Fastest Processor
 */
public class SCFPMain {

    public static void main(String[] args) {
        try {
            int numUser = 1;
            Calendar calendar = Calendar.getInstance();
            boolean traceFlag = false;

            // Initialize CloudSim
            CloudSim.init(numUser, calendar, traceFlag);

            // Create Datacenter
            Datacenter datacenter0 = createDatacenter("Datacenter_0");

            // Create Broker (SCFP)
            SCFPBroker broker = new SCFPBroker("SCFP_Broker");
            int brokerId = broker.getId();

            // Create VMs
            List<Vm> vmList = new ArrayList<>();
            int[] mipsValues = {1000, 2500, 4000}; // VM speeds
            for (int i = 0; i < 3; i++) {
                Vm vm = new Vm(
                        i, brokerId,
                        mipsValues[i], // MIPS
                        1, 2048, 1000, 10000,
                        "Xen", new CloudletSchedulerTimeShared()
                );
                vmList.add(vm);
            }
            broker.submitVmList(vmList);

            // Create Cloudlets
            List<Cloudlet> cloudletList = new ArrayList<>();
            UtilizationModel utilizationModel = new UtilizationModelFull();
            Random rand = new Random(42);
            int numCloudlets = 20;

            for (int i = 0; i < numCloudlets; i++) {
                int length = 3000 + rand.nextInt(9000); // 3000–12000
                Cloudlet cloudlet = new Cloudlet(
                        i, length, 1, 300, 300,
                        utilizationModel, utilizationModel, utilizationModel
                );
                cloudlet.setUserId(brokerId);
                cloudletList.add(cloudlet);
            }
            broker.submitCloudletList(cloudletList);

            // Start Simulation
            CloudSim.startSimulation();
            List<Cloudlet> resultList = broker.getCloudletReceivedList();
            CloudSim.stopSimulation();

            // Print Results
            printCloudletResults(resultList, vmList);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Datacenter with 3 PEs (enough for all VMs)
    private static Datacenter createDatacenter(String name) {
        List<Host> hostList = new ArrayList<>();
        List<Pe> peList = new ArrayList<>();

        // Each PE can handle a VM MIPS
        peList.add(new Pe(0, new PeProvisionerSimple(4000))); // for VM #2
        peList.add(new Pe(1, new PeProvisionerSimple(2500))); // for VM #1
        peList.add(new Pe(2, new PeProvisionerSimple(1000))); // for VM #0

        hostList.add(new Host(
                0,
                new RamProvisionerSimple(8192),
                new BwProvisionerSimple(10000),
                1000000,
                peList,
                new VmSchedulerTimeShared(peList)
        ));

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                "x86", "Linux", "Xen", hostList,
                10.0, 3.0, 0.05, 0.001, 0.0
        );

        try {
            return new Datacenter(
                    name,
                    characteristics,
                    new VmAllocationPolicySimple(hostList),
                    new LinkedList<Storage>(),
                    0
            );
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void printCloudletResults(List<Cloudlet> list, List<Vm> vmList) {
        double makespan = 0.0, totalExecTime = 0.0;
        Map<Integer, Integer> vmLoad = new HashMap<>();

        System.out.println("\n========== OUTPUT ==========");
        System.out.printf("%-10s %-10s %-10s %-15s %-15s %-15s%n",
                "Cloudlet", "Status", "VM", "Start Time", "Finish Time", "Exec Time");

        for (Cloudlet cloudlet : list) {
            if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
                double execTime = cloudlet.getFinishTime() - cloudlet.getExecStartTime();
                makespan = Math.max(makespan, cloudlet.getFinishTime());
                totalExecTime += execTime;

                vmLoad.put(cloudlet.getVmId(), vmLoad.getOrDefault(cloudlet.getVmId(), 0) + 1);

                System.out.printf("%-10d %-10s %-10d %-15.2f %-15.2f %-15.2f%n",
                        cloudlet.getCloudletId(), "SUCCESS",
                        cloudlet.getVmId(),
                        cloudlet.getExecStartTime(),
                        cloudlet.getFinishTime(),
                        execTime);
            }
        }

        // Metrics
        double avgExecTime = totalExecTime / list.size();
        double throughput = list.size() / makespan;

        // Load balance
        double meanLoad = vmLoad.values().stream().mapToInt(Integer::intValue).average().orElse(0.0);
        double variance = vmLoad.values().stream()
                .mapToDouble(v -> Math.pow(v - meanLoad, 2))
                .sum() / vmLoad.size();
        double loadStdDev = Math.sqrt(variance);

        System.out.println("\n========== SCFP METRICS ==========");
        System.out.printf("Makespan: %.2f sec%n", makespan);
        System.out.printf("Average Execution Time: %.2f sec%n", avgExecTime);
        System.out.printf("Throughput: %.2f Cloudlets/sec%n", throughput);
        System.out.printf("VM Load Std Dev (Load Balance): %.4f%n", loadStdDev);
        System.out.println("===================================");
    }
}
