package org.cloudbus.cloudsim.examples.research;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;
import java.text.DecimalFormat;
import java.util.*;

/**
 * PairBasedMainPTS — example main that demonstrates PTS scheduling from the paper.
 * It generates tasks with ST/D/ET metadata, creates VMs and datacenter, and runs the PairBasedBrokerPTS.
 */
public class PairBasedMain{

    public static void main(String[] args) {
        try {
            CloudSim.init(1, Calendar.getInstance(), false);

            Datacenter datacenter0 = createDatacenter("Datacenter_0");
            PairBasedBroker broker = new PairBasedBroker("PairBased_PTS_Broker");
            int brokerId = broker.getId();

            // Create VMs (same values as your project)
            List<Vm> vmList = new ArrayList<>();
            int[] mipsValues = {1000, 2500, 4000};
            for (int i = 0; i < mipsValues.length; i++) {
                Vm vm = new Vm(i, brokerId, mipsValues[i], 1, 2048, 1000, 10000,
                        "Xen", new CloudletSchedulerTimeShared());
                vmList.add(vm);
            }
            broker.submitVmList(vmList);

            // Create Cloudlets (we will attach ST/D to them via broker method)
            List<Cloudlet> cloudletList = new ArrayList<>();
            UtilizationModel utilizationModel = new UtilizationModelFull();

            // We will produce deterministic ST and D values (replace with your real dataset if available)
            // generateExampleTasks returns maps keyed by cloudletId -> startTimeStr ("HH:mm") and duration minutes
            int numCloudlets = 20; // must be even for PTS pair-splitting
            Map<Integer, String> startTimeMap = new HashMap<>();
            Map<Integer, Integer> durationMinMap = new HashMap<>();
            generateExampleTasks(numCloudlets, startTimeMap, durationMinMap);

            Random rand = new Random(42);
            for (int i = 0; i < numCloudlets; i++) {
                // Length is workload in Million Instructions (this is separate from ST/D)
                int length = 3000 + rand.nextInt(9000); // 3k - 12k MI
                Cloudlet cloudlet = new Cloudlet(i, length, 1, 300, 300,
                        utilizationModel, utilizationModel, utilizationModel);
                cloudlet.setUserId(brokerId);
                cloudletList.add(cloudlet);
            }
            broker.submitCloudletList(cloudletList);

            // Send task time metadata to broker (ST in "HH:mm", duration in minutes)
            broker.setTaskTimes(startTimeMap, durationMinMap);

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

    // Example deterministic generator for ST and duration (minutes).
    // You should replace this with real dataset or parsed values if available.
    private static void generateExampleTasks(int n, Map<Integer,String> start, Map<Integer,Integer> dur) {
        // We'll generate times across the day, deterministic pattern.
        // For variety make first half earlier, second half later (so pairing is meaningful).
        String[] sampleStarts = {
                "08:30","11:00","15:00","18:00","22:00",
                "09:00","13:00","17:00","19:45","21:30"
        };
        int[] sampleDur = {75,90,75,90,90,60,75,75,90,135}; // minutes (examples)

        for (int i = 0; i < n; i++) {
            int idx = i % sampleStarts.length;
            start.put(i, sampleStarts[idx]);
            dur.put(i, sampleDur[idx]);
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
                new LinkedList<Storage>(), 0);
    }

    private static void printCloudletList(List<Cloudlet> list) {
        java.text.DecimalFormat dft = new java.text.DecimalFormat("###.##");
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

        System.out.println("\n========== PAIR-BASED PTS METRICS ==========");
        System.out.printf("Makespan: %.2f sec%n", makespan);
        System.out.printf("Average Execution Time: %.2f sec%n", avgExec);
        System.out.printf("Average Waiting Time: %.2f sec%n", avgWait);
        System.out.printf("Throughput: %.2f Cloudlets/sec%n", throughput);
        System.out.printf("VM Load Std Dev: %.2f%n", stdDev);
        System.out.println("========================================");
    }
}
