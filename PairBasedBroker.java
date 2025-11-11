package org.cloudbus.cloudsim.examples.research;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import java.util.*;

public class PairBasedBroker extends DatacenterBroker {

    public PairBasedBroker(String name) throws Exception {
        super(name);
    }

    @Override
    protected void submitCloudletsToDatacenter() {
        List<Cloudlet> allCloudlets = getCloudletList();
        List<Vm> vmList = getVmList();

        if (allCloudlets.isEmpty() || vmList.isEmpty()) {
            return;
        }

        int mid = allCloudlets.size() / 2;
        List<Cloudlet> group1 = new ArrayList<>(allCloudlets.subList(0, mid));
        List<Cloudlet> group2 = new ArrayList<>(allCloudlets.subList(mid, allCloudlets.size()));

        // Deterministic cost matrix for pairing (cloudlet length difference)
        double[][] costMatrix = new double[group1.size()][group2.size()];
        for (int i = 0; i < group1.size(); i++) {
            for (int j = 0; j < group2.size(); j++) {
                costMatrix[i][j] = Math.abs(group1.get(i).getCloudletLength() - group2.get(j).getCloudletLength());
            }
        }

        // Hungarian algorithm for pairing
        int[] assignment = HungarianAlgorithm.computeAssignments(costMatrix);

        // Assign paired cloudlets to VMs in round-robin
        int vmIndex = 0;
        List<Cloudlet> scheduledCloudlets = new ArrayList<>();

        for (int i = 0; i < assignment.length; i++) {
            int j = assignment[i];
            if (j >= 0) {
                Cloudlet c1 = group1.get(i);
                Cloudlet c2 = group2.get(j);

                c1.setVmId(vmList.get(vmIndex % vmList.size()).getId());
                c2.setVmId(vmList.get(vmIndex % vmList.size()).getId());
                vmIndex++;

                scheduledCloudlets.add(c1);
                scheduledCloudlets.add(c2);
            }
        }

        // Submit the paired cloudlets properly
        for (Cloudlet c : scheduledCloudlets) {
            super.submitCloudlet(c);
            
        }
    }
}
