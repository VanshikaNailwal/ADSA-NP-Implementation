package org.cloudbus.cloudsim.examples.research;

public class HungarianAlgorithm {

    // Solve the assignment problem using the Hungarian Algorithm
    // Return array: result[i] = j => task i assigned to task j
    public static int[] computeAssignments(double[][] costMatrix) {
        int n = costMatrix.length;
        int m = costMatrix[0].length;
        int size = Math.max(n, m);

        double[][] cost = new double[size][size];
        for (int i = 0; i < size; i++)
            for (int j = 0; j < size; j++)
                cost[i][j] = (i < n && j < m) ? costMatrix[i][j] : 0;

        // Step 1: Subtract row minimum
        for (int i = 0; i < size; i++) {
            double min = cost[i][0];
            for (int j = 1; j < size; j++) min = Math.min(min, cost[i][j]);
            for (int j = 0; j < size; j++) cost[i][j] -= min;
        }

        // Step 2: Subtract column minimum
        for (int j = 0; j < size; j++) {
            double min = cost[0][j];
            for (int i = 1; i < size; i++) min = Math.min(min, cost[i][j]);
            for (int i = 0; i < size; i++) cost[i][j] -= min;
        }

        int[] result = new int[n];
        boolean[] rowCovered = new boolean[size];
        boolean[] colCovered = new boolean[size];
        int[][] stars = new int[size][size]; // 1 = starred zero

        // Step 3: Star zeros
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                if (cost[i][j] == 0 && !rowCovered[i] && !colCovered[j]) {
                    stars[i][j] = 1;
                    rowCovered[i] = true;
                    colCovered[j] = true;
                }
            }
        }

        // Reset cover arrays
        java.util.Arrays.fill(rowCovered, false);
        java.util.Arrays.fill(colCovered, false);

        // Step 4-6: Simple assignment
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                if (stars[i][j] == 1) {
                    result[i] = j;
                    break;
                }
            }
        }
        return result;
    }
}
