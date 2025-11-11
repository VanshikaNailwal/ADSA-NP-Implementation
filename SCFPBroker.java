package org.cloudbus.cloudsim.examples.research;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSimTags;
import java.util.*;

public class SCFPBroker extends DatacenterBroker {

    public SCFPBroker(String name) throws Exception {
        super(name);
    }

    @Override
    protected void submitCloudlets() {
        List<Cloudlet> cloudletList = getCloudletList();
        List<Vm> vmList = getVmList();

        if (cloudletList == null || vmList == null || vmList.isEmpty()) {
            System.out.println(getName() + ": No Cloudlets or VMs available.");
            return;
        }

        System.out.println("\n========== SCFP Scheduling Started ==========");
        System.out.println("Total Cloudlets: " + cloudletList.size());
        System.out.println("Total VMs: " + vmList.size());

        // 1. Sort cloudlets ascending (shortest first)
        cloudletList.sort(Comparator.comparingLong(Cloudlet::getCloudletLength));

        // 2. Sort VMs descending (fastest first)
        vmList.sort((v1, v2) -> Double.compare(v2.getMips(), v1.getMips()));

        // 3. Track next available time for each VM
        Map<Integer, Double> vmNextAvailableTime = new HashMap<>();
        for (Vm vm : vmList) {
            vmNextAvailableTime.put(vm.getId(), 0.0);
        }

        // 4. Assign cloudlets to VMs
        for (Cloudlet cloudlet : cloudletList) {
            // Pick VM that becomes free first
            Vm selectedVm = null;
            double earliestTime = Double.MAX_VALUE;

            for (Vm vm : vmList) {
                double availableTime = vmNextAvailableTime.get(vm.getId());
                if (availableTime < earliestTime) {
                    earliestTime = availableTime;
                    selectedVm = vm;
                }
            }

            cloudlet.setVmId(selectedVm.getId());

            Integer datacenterId = getVmsToDatacentersMap().get(selectedVm.getId());
            if (datacenterId != null) {
                sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                cloudletsSubmitted++;
            }

            // Update VM's next available time
            double execTime = cloudlet.getCloudletLength() / selectedVm.getMips();
            vmNextAvailableTime.put(selectedVm.getId(), earliestTime + execTime);

            System.out.printf("Assigned Cloudlet #%d (Length: %d) → VM #%d (MIPS: %.2f)%n",
                    cloudlet.getCloudletId(),
                    cloudlet.getCloudletLength(),
                    selectedVm.getId(),
                    selectedVm.getMips());
        }

        setCloudletSubmittedList(cloudletList);
        cloudletList.clear();

        System.out.println("========== SCFP Scheduling Completed ==========\n");
    }
}
