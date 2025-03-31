package org.supreme.models;

import java.util.Set;

import lombok.Data;

@Data
public class UserInterestsAggr {
    

    public String userId;
    public Set<String> interests;
    public long ws;
    public long we;
}
