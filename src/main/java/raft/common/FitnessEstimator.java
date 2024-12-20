package raft.common;

public abstract class FitnessEstimator {
    private double fitness;

    public double getFitness() {
        return fitness;
    }

    public void setFitness(float fitness) {
        this.fitness = fitness;
        System.out.printf("[Fitness Estimator] New fitness: %f\n", fitness);
    }
}
