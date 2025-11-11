package org.cloudbus.cloudsim.examples.research;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;
import java.util.*;

public class AllAlgoComparison {

    static class Metrics {
        double makespan;
        double avgExec;
        double avgWait;
        double throughput;
        double loadBalance;

        public Metrics(double makespan, double avgExec, double avgWait, double throughput, double loadBalance) {
            this.makespan = makespan;
            this.avgExec = avgExec;
            this.avgWait = avgWait;
            this.throughput = throughput;
            this.loadBalance = loadBalance;
        }
    }

    public static void main(String[] args) {
        System.out.println("Starting comparison of 4 scheduling algorithms...");

        try {
            int numUser = 1;
            Calendar calendar = Calendar.getInstance();
            boolean traceFlag = false;

            // Run all algorithms sequentially
            Metrics lcfpMetrics = runAlgorithm("LCFP");
            Metrics scfpMetrics = runAlgorithm("SCFP");
            Metrics pairMetrics = runAlgorithm("Pair-Based");
            Metrics emmMetrics = runAlgorithm("Enhanced Max-Min");

            // Print comparison table
            System.out.println("\n================== Comparison of Algorithms ==================");
            System.out.printf("%-15s %-10s %-15s %-15s %-15s %-15s%n",
                    "Algorithm", "Makespan", "Avg Exec Time", "Avg Wait Time", "Throughput", "VM Load StdDev");
            System.out.printf("%-15s %-10.2f %-15.2f %-15.2f %-15.2f %-15.2f%n",
                    "LCFP", lcfpMetrics.makespan, lcfpMetrics.avgExec, lcfpMetrics.avgWait,
                    lcfpMetrics.throughput, lcfpMetrics.loadBalance);
            System.out.printf("%-15s %-10.2f %-15.2f %-15.2f %-15.2f %-15.2f%n",
                    "SCFP", scfpMetrics.makespan, scfpMetrics.avgExec, scfpMetrics.avgWait,
                    scfpMetrics.throughput, scfpMetrics.loadBalance);
            System.out.printf("%-15s %-10.2f %-15.2f %-15.2f %-15.2f %-15.2f%n",
                    "Pair-Based", pairMetrics.makespan, pairMetrics.avgExec, pairMetrics.avgWait,
                    pairMetrics.throughput, pairMetrics.loadBalance);
            System.out.printf("%-15s %-10.2f %-15.2f %-15.2f %-15.2f %-15.2f%n",
                    "Enhanced-MM", emmMetrics.makespan, emmMetrics.avgExec, emmMetrics.avgWait,
                    emmMetrics.throughput, emmMetrics.loadBalance);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Metrics runAlgorithm(String algoName) throws Exception {
        // 1. Initialize CloudSim
        CloudSim.init(1, Calendar.getInstance(), false);

        // 2. Create Datacenter
        Datacenter datacenter0 = createDatacenter("Datacenter_0");

        // 3. Create Broker based on algoName
        DatacenterBroker broker;
        switch (algoName) {
            case "LCFP":
                broker = new LCFPBroker("LCFP_Broker");
                break;
            case "SCFP":
                broker = new SCFPBroker("SCFP_Broker");
                break;
            case "Pair-Based":
                broker = new PairBasedBroker("Pair_Based_Broker");
                break;
            case "Enhanced Max-Min":
                broker = new EnhancedMaxMinBroker("EnhancedMM_Broker");
                break;
            default:
                throw new IllegalArgumentException("Unknown algorithm: " + algoName);
        }

        int brokerId = broker.getId();

        // 4. Create VMs
        List<Vm> vmList = new ArrayList<>();
        int[] mips = {1000, 2500, 4000};
        for (int i = 0; i < 3; i++) {
            Vm vm = new Vm(i, brokerId, mips[i], 1, 2048, 1000, 10000, "Xen",
                    new CloudletSchedulerTimeShared());
            vmList.add(vm);
        }
        broker.submitVmList(vmList);

        // 5. Create Cloudlets
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

        // 6. Start simulation
        CloudSim.startSimulation();
        CloudSim.stopSimulation();

        // 7. Collect metrics
        List<Cloudlet> receivedList = broker.getCloudletReceivedList();
        double makespan = 0, totalExec = 0, totalWait = 0;
        double[] vmWorkload = new double[vmList.size()];

        for (Cloudlet c : receivedList) {
            makespan = Math.max(makespan, c.getFinishTime());
            totalExec += c.getActualCPUTime();
            totalWait += c.getExecStartTime();
            vmWorkload[c.getVmId()] += c.getActualCPUTime();
        }

        double avgExec = totalExec / receivedList.size();
        double avgWait = totalWait / receivedList.size();
        double throughput = receivedList.size() / makespan;

        // Compute VM load standard deviation
        double mean = 0;
        for (double w : vmWorkload) mean += w;
        mean /= vmWorkload.length;
        double stdDev = 0;
        for (double w : vmWorkload) stdDev += Math.pow(w - mean, 2);
        stdDev = Math.sqrt(stdDev / vmWorkload.length);

        return new Metrics(makespan, avgExec, avgWait, throughput, stdDev);
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
}
