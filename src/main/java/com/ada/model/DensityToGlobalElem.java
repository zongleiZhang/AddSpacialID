package com.ada.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class DensityToGlobalElem implements Serializable {
    public Integer key;
    public DensityToGlobalValue value;

    public DensityToGlobalElem() {}

    public DensityToGlobalElem(Integer key, DensityToGlobalValue value) {
        this.key = key;
        this.value = value;
    }
}
