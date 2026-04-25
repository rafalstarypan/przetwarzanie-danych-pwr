package pl.pwr.bigdata.mr.common;

public final class LinearAlgebra {

    public static class SingularMatrixException extends Exception {
        public SingularMatrixException(String msg) { super(msg); }
    }

    private LinearAlgebra() {}

    public static double[] solve(double[][] aIn, double[] bIn) throws SingularMatrixException {
        int n = bIn.length;
        double[][] a = new double[n][n + 1];
        for (int i = 0; i < n; i++) {
            System.arraycopy(aIn[i], 0, a[i], 0, n);
            a[i][n] = bIn[i];
        }

        for (int p = 0; p < n; p++) {
            int max = p;
            for (int i = p + 1; i < n; i++) {
                if (Math.abs(a[i][p]) > Math.abs(a[max][p])) max = i;
            }
            double[] tmp = a[p]; a[p] = a[max]; a[max] = tmp;

            if (Math.abs(a[p][p]) < 1e-12) {
                throw new SingularMatrixException("singular at pivot " + p);
            }

            for (int i = p + 1; i < n; i++) {
                double alpha = a[i][p] / a[p][p];
                for (int j = p; j <= n; j++) {
                    a[i][j] -= alpha * a[p][j];
                }
            }
        }

        double[] x = new double[n];
        for (int i = n - 1; i >= 0; i--) {
            double sum = 0.0;
            for (int j = i + 1; j < n; j++) sum += a[i][j] * x[j];
            x[i] = (a[i][n] - sum) / a[i][i];
        }
        return x;
    }
}
