package org.cloudbus.cloudsim.examples.research;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;

import java.util.*;

public class EnhancedMaxMinBroker extends DatacenterBroker {

    public EnhancedMaxMinBroker(String name) throws Exception {
        super(name);
    }

    @Override
    protected void submitCloudlets() {
        if (getCloudletList() == null || getCloudletList().isEmpty()) return;
        if (getVmList() == null || getVmList().isEmpty()) return;

        System.out.println("\n========== Enhanced Max-Min Scheduling Started ==========");
        System.out.println("Total Cloudlets: " + getCloudletList().size());
        System.out.println("Total Active VMs: " + getVmList().size());

        // Initialize VM ready times
        Map<Integer, Double> vmReadyTime = new HashMap<>();
        for (Vm vm : getVmList()) vmReadyTime.put(vm.getId(), 0.0);

        List<Cloudlet> unscheduledCloudlets = new ArrayList<>(getCloudletList());

        while (!unscheduledCloudlets.isEmpty()) {

            // Step 2: Compute execution and completion times
            Map<Cloudlet, Map<Vm, Double>> completionTimeMap = new HashMap<>();
            for (Cloudlet c : unscheduledCloudlets) {
                Map<Vm, Double> vmTimes = new HashMap<>();
                for (Vm vm : getVmList()) {
                    double execTime = c.getCloudletLength() / vm.getMips(); // Eij
                    double compTime = vmReadyTime.get(vm.getId()) + execTime; // Cij
                    vmTimes.put(vm, compTime);
                }
                completionTimeMap.put(c, vmTimes);
            }

            // Step 3: Compute average execution time
            double avgExec = unscheduledCloudlets.stream()
                    .mapToDouble(c -> {
                        double sum = 0;
                        for (Vm vm : getVmList()) sum += c.getCloudletLength() / vm.getMips();
                        return sum / getVmList().size();
                    }).average().orElse(0.0);

            // Step 4: Select cloudlet closest to or above average
            Cloudlet selected = null;
            double diff = Double.MAX_VALUE;
            for (Cloudlet c : unscheduledCloudlets) {
                double cAvgExec = completionTimeMap.get(c).values().stream().mapToDouble(v -> v).average().orElse(0);
                if (cAvgExec >= avgExec && (cAvgExec - avgExec) < diff) {
                    selected = c;
                    diff = cAvgExec - avgExec;
                }
            }
            // if none above average, pick the largest cloudlet
            if (selected == null) {
                selected = unscheduledCloudlets.stream()
                        .max(Comparator.comparingLong(Cloudlet::getCloudletLength))
                        .orElse(unscheduledCloudlets.get(0));
            }

            // Step 4: Assign to VM with minimum completion time
            Vm bestVm = null;
            double minComp = Double.MAX_VALUE;
            for (Map.Entry<Vm, Double> entry : completionTimeMap.get(selected).entrySet()) {
                if (entry.getValue() < minComp) {
                    minComp = entry.getValue();
                    bestVm = entry.getKey();
                }
            }

            // Assign cloudlet to VM
            selected.setVmId(bestVm.getId());
            vmReadyTime.put(bestVm.getId(), minComp);

            System.out.printf("Assigned Cloudlet #%d (Length: %d) → VM #%d (MIPS: %.2f), Completion: %.2f%n",
                    selected.getCloudletId(), selected.getCloudletLength(),
                    bestVm.getId(), bestVm.getMips(), minComp);

            // Step 5: Remove cloudlet from unscheduled list
            unscheduledCloudlets.remove(selected);
        }

        // Submit all scheduled cloudlets to their datacenter
        for (Cloudlet cloudlet : getCloudletList()) {
            int vmId = cloudlet.getVmId();
            Integer datacenterId = getVmsToDatacentersMap().get(vmId);
            if (datacenterId == null) continue;
            sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
            cloudletsSubmitted++;
        }

        setCloudletSubmittedList(getCloudletList());
        getCloudletList().clear();

        System.out.println("========== Enhanced Max-Min Scheduling Completed ==========\n");
    }
}
