package org.cloudbus.cloudsim.examples.research;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.provisioners.*;
import java.text.DecimalFormat;
import java.util.*;

public class PairBasedMain {

    public static void main(String[] args) {
        try {
            CloudSim.init(1, Calendar.getInstance(), false);

            Datacenter datacenter0 = createDatacenter("Datacenter_0");
            PairBasedBroker broker = new PairBasedBroker("PairBased_Broker");
            int brokerId = broker.getId();

            // Create VMs
            List<Vm> vmList = new ArrayList<>();
            int[] mipsValues = {1000, 2500, 4000};
            for (int i = 0; i < 3; i++) {
                vmList.add(new Vm(i, brokerId, mipsValues[i], 1, 2048, 1000, 10000,
                        "Xen", new CloudletSchedulerTimeShared()));
            }
            broker.submitVmList(vmList);

            // Create Cloudlets with fixed seed
            List<Cloudlet> cloudletList = new ArrayList<>();
            UtilizationModel utilizationModel = new UtilizationModelFull();
            Random rand = new Random(42); // fixed seed
            for (int i = 0; i < 20; i++) {
                int length = 3000 + rand.nextInt(9000);
                Cloudlet cloudlet = new Cloudlet(i, length, 1, 300, 300,
                        utilizationModel, utilizationModel, utilizationModel);
                cloudlet.setUserId(brokerId);
                cloudletList.add(cloudlet);
            }
            broker.submitCloudletList(cloudletList);

            // Start Simulation
            CloudSim.startSimulation();
            List<Cloudlet> resultList = broker.getCloudletReceivedList();
            CloudSim.stopSimulation();

            // Print results
            printCloudletList(resultList);
            printMetrics(resultList, vmList);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Datacenter createDatacenter(String name) throws Exception {
        List<Pe> peList = new ArrayList<>();
        peList.add(new Pe(0, new PeProvisionerSimple(4000)));
        peList.add(new Pe(1, new PeProvisionerSimple(4000)));
        peList.add(new Pe(2, new PeProvisionerSimple(4000)));

        List<Host> hostList = new ArrayList<>();
        hostList.add(new Host(0,
                new RamProvisionerSimple(8192),
                new BwProvisionerSimple(10000),
                1000000, peList,
                new VmSchedulerTimeShared(peList)));

        return new Datacenter(name,
                new DatacenterCharacteristics("x86", "Linux", "Xen", hostList,
                        10.0, 3.0, 0.05, 0.001, 0.0),
                new VmAllocationPolicySimple(hostList),
                new LinkedList<>(), 0);
    }

    private static void printCloudletList(List<Cloudlet> list) {
        DecimalFormat dft = new DecimalFormat("###.##");
        System.out.println("\n========== CLOUDLET RESULTS ==========");
        System.out.printf("%-10s %-10s %-10s %-15s %-15s %-10s%n",
                "Cloudlet", "Status", "VM", "Start Time", "Finish Time", "Exec Time");

        for (Cloudlet c : list) {
            System.out.printf("%-10d %-10s %-10d %-15s %-15s %-10s%n",
                    c.getCloudletId(),
                    (c.getStatus() == Cloudlet.SUCCESS ? "SUCCESS" : "FAILED"),
                    c.getVmId(),
                    dft.format(c.getExecStartTime()),
                    dft.format(c.getFinishTime()),
                    dft.format(c.getActualCPUTime()));
        }
    }

    private static void printMetrics(List<Cloudlet> list, List<Vm> vmList) {
        double makespan = list.stream().mapToDouble(Cloudlet::getFinishTime).max().orElse(0.0);
        double avgExec = list.stream().mapToDouble(Cloudlet::getActualCPUTime).average().orElse(0.0);
        double avgWait = list.stream().mapToDouble(Cloudlet::getExecStartTime).average().orElse(0.0);
        double throughput = (makespan > 0) ? list.size() / makespan : 0.0;

        Map<Integer, Double> vmWorkload = new HashMap<>();
        for (Vm vm : vmList) vmWorkload.put(vm.getId(), 0.0);
        for (Cloudlet c : list)
            vmWorkload.put(c.getVmId(), vmWorkload.get(c.getVmId()) + c.getActualCPUTime());
        double avgLoad = vmWorkload.values().stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double stdDev = Math.sqrt(vmWorkload.values().stream()
                .mapToDouble(v -> Math.pow(v - avgLoad, 2)).average().orElse(0));

        System.out.println("\n========== PAIR-BASED METRICS ==========");
        System.out.printf("Makespan: %.2f sec%n", makespan);
        System.out.printf("Average Execution Time: %.2f sec%n", avgExec);
        System.out.printf("Average Waiting Time: %.2f sec%n", avgWait);
        System.out.printf("Throughput: %.2f Cloudlets/sec%n", throughput);
        System.out.printf("VM Load Std Dev: %.2f%n", stdDev);
        System.out.println("========================================");
    }
}
