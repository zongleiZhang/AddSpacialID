package com.ada.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;

@Getter
@Setter
public class Density implements DensityToGlobalValue, Serializable {
    public int[][] grids;

    public Density() {}

    public Density(int[][] grids) {
        this.grids = grids;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(grids);
    }
}
