package org.cloudbus.cloudsim.examples.research;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import java.util.*;

/**
 * PairBasedBrokerPTS — Broker implementing PTS algorithm from the paper
 * Assumptions:
 *  - The main provides maps for each cloudlet id:
 *      startTimeMap: map<cloudletId, "HH:mm">
 *      durationMinMap: map<cloudletId, durationInMinutes>
 *
 *  - The broker uses these to compute ET (end time), lease time, conversions
 *    and then solves pairing using row/column opportunity matrices + Hungarian-like procedure.
 */
public class PairBasedBroker extends DatacenterBroker {

    // task metadata maps
    private Map<Integer, String> startTimeMap = new HashMap<>();
    private Map<Integer, Integer> durationMinMap = new HashMap<>();

    public PairBasedBroker(String name) throws Exception {
        super(name);
    }

    /**
     * Called by main to set task start times ("HH:mm") and durations (minutes).
     * Must be called before startSimulation() or before broker schedules cloudlets.
     */
    public void setTaskTimes(Map<Integer, String> startTimes, Map<Integer, Integer> durations) {
        if (startTimes != null) this.startTimeMap.putAll(startTimes);
        if (durations != null) this.durationMinMap.putAll(durations);
    }

    // Core scheduling method override — paper calls this after VM creation
    @Override
    protected void submitCloudletsToDatacenter() {
        List<Cloudlet> allCloudlets = getCloudletList();
        List<Vm> vmList = getVmList();

        if (allCloudlets == null || allCloudlets.isEmpty() || vmList == null || vmList.isEmpty()) {
            return;
        }

        // Ensure even number of tasks (paper splits into two equal groups)
        if (allCloudlets.size() % 2 != 0) {
            // If odd, drop last for pairing (or you can pad). Here we log and drop last.
            System.out.println(getName()+": odd number of cloudlets - dropping last for PTS pairing.");
            allCloudlets.remove(allCloudlets.size()-1);
        }

        // Build maps: ET (end time in minutes from midnight) for each cloudlet
        Map<Integer, Integer> startMin = new HashMap<>();
        Map<Integer, Integer> endMin = new HashMap<>();
        for (Cloudlet c : allCloudlets) {
            int id = c.getCloudletId();
            String st = startTimeMap.get(id);
            Integer dur = durationMinMap.get(id);
            if (st == null || dur == null) {
                // Fallback: if metadata not provided, assign a reasonable default based on order
                // default start 08:00 + (id * 30min)
                int fallbackStart = (8*60) + (id * 30);
                startMin.put(id, fallbackStart % (24*60));
                endMin.put(id, (fallbackStart + 60) % (24*60)); // default 60 min
            } else {
                int s = parseHHMMToMinutes(st);
                startMin.put(id, s);
                endMin.put(id, (s + dur) % (24*60));
            }
        }

        // split into group1 and group2 (first half and last half by order in allCloudlets list)
        int mid = allCloudlets.size() / 2;
        List<Cloudlet> g1 = new ArrayList<>(allCloudlets.subList(0, mid));
        List<Cloudlet> g2 = new ArrayList<>(allCloudlets.subList(mid, allCloudlets.size()));

        int n1 = g1.size(), n2 = g2.size();
        int n = Math.max(n1, n2);

        // Build LT and CLT matrices (in minutes)
        double[][] LT_minutes = new double[n1][n2];
        double[][] CLT_minutes = new double[n1][n2];

        for (int i = 0; i < n1; i++) {
            for (int j = 0; j < n2; j++) {
                int id1 = g1.get(i).getCloudletId();
                int id2 = g2.get(j).getCloudletId();
                int et1 = endMin.get(id1);
                int st2 = startMin.get(id2);
                int et2 = endMin.get(id2);
                int st1 = startMin.get(id1);

                // Lease time LT = ET(j) - ST(j') as paper defined (j from G1 to j' in G2)
                int lt = et1 - st2;
                // If lease < 0, wrap by adding 24h (paper's ±24h treatment will handle)
                if (lt < 0) lt += 24*60;
                LT_minutes[i][j] = lt;

                // Converse lease time CLT = ET(j') - ST(j)
                int clt = et2 - st1;
                if (clt < 0) clt += 24*60;
                CLT_minutes[i][j] = clt;
            }
        }

        // Paper: if lease < transferTime (1h) adjust with +/-24h depending on ET and ST order.
        // We will follow the interpretation: if lt < transfer(60), then if ET>ST add/subtract 24h as paper describes.
        // The paper's exact minute-level adjustments are a bit complex; we implement the logical effect:
        //    if (lt < 60) adjust to represent that 'waiting' crosses midnight as described.
        for (int i = 0; i < n1; i++) {
            for (int j = 0; j < n2; j++) {
                int id1 = g1.get(i).getCloudletId();
                int id2 = g2.get(j).getCloudletId();
                int et1 = endMin.get(id1);
                int st2 = startMin.get(id2);
                int et2 = endMin.get(id2);
                int st1 = startMin.get(id1);

                // Lease time adjustment
                if (LT_minutes[i][j] < 60) {
                    // if ET1 > ST2 then subtract LT from 24h
                    if (et1 > st2) {
                        LT_minutes[i][j] = (24*60) - (st2 - et1);
                    } else {
                        LT_minutes[i][j] = (24*60) + (st2 - et1);
                    }
                }
                // Converse lease adjustment
                if (CLT_minutes[i][j] < 60) {
                    if (et2 > st1) {
                        CLT_minutes[i][j] = (24*60) - (st1 - et2);
                    } else {
                        CLT_minutes[i][j] = (24*60) + (st1 - et2);
                    }
                }
            }
        }

        // Convert LT and CLT to whole-number units (paper: scale hour/min to 0..100 then convert to units such that 1 hour = 4 units)
        // Implementation detail (paper uses a scaling trick): We'll compute units = round((hours*100 + scaledMinutes) / 25)
        // Equivalent: 1 hour => 4 units. We'll compute fractional hours -> units directly:
        int[][] LT_units = new int[n1][n2];
        int[][] CLT_units = new int[n1][n2];
        for (int i = 0; i < n1; i++) {
            for (int j = 0; j < n2; j++) {
                LT_units[i][j] = minutesToUnits((int)Math.round(LT_minutes[i][j]));
                CLT_units[i][j] = minutesToUnits((int)Math.round(CLT_minutes[i][j]));
            }
        }

        // Build least time matrix (TOM preliminary) = min(LT_units, CLT_units)
        int[][] leastUnits = new int[n1][n2];
        for (int i = 0; i < n1; i++) {
            for (int j = 0; j < n2; j++) {
                leastUnits[i][j] = Math.min(LT_units[i][j], CLT_units[i][j]);
            }
        }

        // Convert leastUnits to a double matrix for reduction steps
        double[][] matrix = new double[n][n];
        // initialize with large numbers for padded cells if n1 != n2
        double INF = 1e9;
        for (int i = 0; i < n; i++)
            Arrays.fill(matrix[i], INF);

        for (int i = 0; i < n1; i++) {
            for (int j = 0; j < n2; j++) {
                matrix[i][j] = leastUnits[i][j];
            }
        }

        // PTS: build ROM (row opportunity) and COM (column opportunity) via row & column reductions
        matrix = rowReduction(matrix, n);
        matrix = columnReduction(matrix, n);

        // Now iteratively apply the "cover zeros" & adjust smallest uncovered element until
        // minimum number of lines to cover all zeros equals number of pairs (nPairs = n1)
        int targetPairs = n1; // number of pairs to form (rows)
        while (true) {
            // create zero mask
            boolean[][] zeroMask = new boolean[n][n];
            for (int i = 0; i < n; i++)
                for (int j = 0; j < n; j++)
                    zeroMask[i][j] = (Math.abs(matrix[i][j]) < 1e-6);

            // compute maximum matching on bipartite graph formed by zeros
            int matchingSize = maxBipartiteMatching(zeroMask, n);

            if (matchingSize >= targetPairs) break; // enough pairings (paper: lines covering zeros == nPairs)

            // find smallest uncovered element
            // to do so we need a covering of rows/cols; use the standard algorithm to get covered rows/cols from matching
            boolean[] coverRows = new boolean[n];
            boolean[] coverCols = new boolean[n];
            computeCoversFromZeroMatching(zeroMask, coverRows, coverCols, n);

            double minUncovered = Double.MAX_VALUE;
            for (int i = 0; i < n; i++) {
                if (coverRows[i]) continue;
                for (int j = 0; j < n; j++) {
                    if (coverCols[j]) continue;
                    if (matrix[i][j] < minUncovered) minUncovered = matrix[i][j];
                }
            }
            if (minUncovered == Double.MAX_VALUE) break;

            // subtract minUncovered from all uncovered elements, add to elements covered twice
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    if (!coverRows[i] && !coverCols[j]) {
                        matrix[i][j] -= minUncovered;
                    } else if (coverRows[i] && coverCols[j]) {
                        matrix[i][j] += minUncovered;
                    }
                }
            }
            // loop until matching size >= targetPairs
        }

        // Extract assignment — perform matching on zero cells and use only first n1 rows
        boolean[][] finalZeroMask = new boolean[n][n];
        for (int i = 0; i < n; i++)
            for (int j = 0; j < n; j++)
                finalZeroMask[i][j] = (Math.abs(matrix[i][j]) < 1e-6);

        int[] pairAssignment = extractPairingFromZeros(finalZeroMask, n, n1, n2);

        // pairAssignment[i] = j means g1[i] pairs with g2[j]
        // Now bind paired cloudlets to VMs in round-robin as paper describes (or you can pick a different mapping)
        int vmIndex = 0;
        List<Cloudlet> scheduled = new ArrayList<>();
        for (int i = 0; i < pairAssignment.length; i++) {
            int j = pairAssignment[i];
            if (j >= 0 && i < g1.size() && j < g2.size()) {
                Cloudlet c1 = g1.get(i);
                Cloudlet c2 = g2.get(j);
                int vmId = vmList.get(vmIndex % vmList.size()).getId();
                // Bind cloudlets to VM (use bindCloudletToVm OR setVmId+ submit via super.submitCloudlet)
                bindCloudletToVm(c1.getCloudletId(), vmId);
                bindCloudletToVm(c2.getCloudletId(), vmId);
                scheduled.add(c1);
                scheduled.add(c2);
                vmIndex++;
            }
        }

        // Submit scheduled cloudlets (use parent's single-cloudlet submission so events are correctly sent)
        for (Cloudlet c : scheduled) {
            super.submitCloudlet(c);
        }
    }

    // ---------- Utility helpers below ----------

    // parse "HH:mm" to minutes from midnight
    private int parseHHMMToMinutes(String hhmm) {
        try {
            String[] parts = hhmm.split(":");
            int hh = Integer.parseInt(parts[0].trim());
            int mm = Integer.parseInt(parts[1].trim());
            return (hh % 24) * 60 + (mm % 60);
        } catch (Exception e) {
            return 0;
        }
    }

    // Convert minutes to paper units (1 hour == 4 units)
    // We follow the paper's approximate approach: units = round(minutes / 15)
    // Because 1 hour = 60 minutes -> 60/15 = 4 units
    private int minutesToUnits(int minutes) {
        if (minutes < 0) minutes = (minutes % (24*60) + 24*60) % (24*60);
        return (int) Math.round(minutes / 15.0);
    }

    // Row reduction: subtract row minima
    private double[][] rowReduction(double[][] mat, int n) {
        for (int i = 0; i < n; i++) {
            double min = Double.MAX_VALUE;
            for (int j = 0; j < n; j++) min = Math.min(min, mat[i][j]);
            if (min == Double.MAX_VALUE || min < 0) continue;
            for (int j = 0; j < n; j++) {
                if (mat[i][j] < Double.MAX_VALUE/2) mat[i][j] -= min;
            }
        }
        return mat;
    }

    // Column reduction: subtract column minima
    private double[][] columnReduction(double[][] mat, int n) {
        for (int j = 0; j < n; j++) {
            double min = Double.MAX_VALUE;
            for (int i = 0; i < n; i++) min = Math.min(min, mat[i][j]);
            if (min == Double.MAX_VALUE || min < 0) continue;
            for (int i = 0; i < n; i++) {
                if (mat[i][j] < Double.MAX_VALUE/2) mat[i][j] -= min;
            }
        }
        return mat;
    }

    // Compute maximum bipartite matching on zeroMask (size n x n). Returns matching size.
    // Uses DFS based augmenting path (standard).
    private int maxBipartiteMatching(boolean[][] zeroMask, int n) {
        int[] matchR = new int[n];
        Arrays.fill(matchR, -1);
        int result = 0;
        for (int u = 0; u < n; u++) {
            boolean[] seen = new boolean[n];
            if (bipartiteDfs(u, zeroMask, seen, matchR, n)) result++;
        }
        return result;
    }

    private boolean bipartiteDfs(int u, boolean[][] zeroMask, boolean[] seen, int[] matchR, int n) {
        for (int v = 0; v < n; v++) {
            if (!zeroMask[u][v] || seen[v]) continue;
            seen[v] = true;
            if (matchR[v] < 0 || bipartiteDfs(matchR[v], zeroMask, seen, matchR, n)) {
                matchR[v] = u;
                return true;
            }
        }
        return false;
    }

    // Compute coverRows and coverCols from zeroMask using greedy method derived from matching:
    // We compute a maximum matching and then derive minimum vertex cover via Kőnig's theorem:
    //  - find unmatched rows, do DFS through alternating paths; coverRows = all rows NOT visited, coverCols = visited columns.
    private void computeCoversFromZeroMatching(boolean[][] zeroMask, boolean[] coverRows, boolean[] coverCols, int n) {
        int[] matchR = new int[n];
        Arrays.fill(matchR, -1);
        int[] matchL = new int[n];
        Arrays.fill(matchL, -1);
        // Build max matching
        for (int u = 0; u < n; u++) {
            boolean[] seen = new boolean[n];
            bipartiteDfsForMatch(u, zeroMask, seen, matchR, matchL, n);
        }
        // Now build alternating tree starting from unmatched rows
        boolean[] visRow = new boolean[n];
        boolean[] visCol = new boolean[n];
        Deque<Integer> dq = new ArrayDeque<>();
        for (int i = 0; i < n; i++) {
            if (matchL[i] == -1) { // unmatched left row
                dq.add(i);
                visRow[i] = true;
            }
        }
        while (!dq.isEmpty()) {
            int r = dq.poll();
            for (int c = 0; c < n; c++) {
                if (!zeroMask[r][c]) continue;
                if (!visCol[c]) {
                    visCol[c] = true;
                    if (matchR[c] != -1 && !visRow[matchR[c]]) {
                        visRow[matchR[c]] = true;
                        dq.add(matchR[c]);
                    }
                }
            }
        }
        // Kőnig's theorem: min vertex cover = (all rows NOT visited) U (all cols visited)
        for (int i = 0; i < n; i++) coverRows[i] = !visRow[i];
        for (int j = 0; j < n; j++) coverCols[j] = visCol[j];
    }

    // helper used by computeCoversFromZeroMatching
    private boolean bipartiteDfsForMatch(int u, boolean[][] zeroMask, boolean[] seen, int[] matchR, int[] matchL, int n) {
        for (int v = 0; v < n; v++) {
            if (!zeroMask[u][v] || seen[v]) continue;
            seen[v] = true;
            if (matchR[v] == -1 || bipartiteDfsForMatch(matchR[v], zeroMask, seen, matchR, matchL, n)) {
                matchR[v] = u;
                matchL[u] = v;
                return true;
            }
        }
        return false;
    }

    // extract pairing (for first n1 rows) from final zeroMask by performing a maximum matching and returning mapping
    private int[] extractPairingFromZeros(boolean[][] zeroMask, int n, int n1, int n2) {
        int[] matchR = new int[n];
        Arrays.fill(matchR, -1);
        int[] matchL = new int[n];
        Arrays.fill(matchL, -1);

        // compute maximum matching
        for (int u = 0; u < n; u++) {
            boolean[] seen = new boolean[n];
            bipartiteDfsForMatch(u, zeroMask, seen, matchR, matchL, n);
        }

        int[] result = new int[n1];
        Arrays.fill(result, -1);
        for (int j = 0; j < n; j++) {
            if (matchR[j] != -1 && matchR[j] < n1 && j < n2) {
                result[matchR[j]] = j;
            }
        }
        return result;
    }
}
