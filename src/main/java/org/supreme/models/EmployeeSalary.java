package org.supreme.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class EmployeeSalary {

    public String id;
    public String name;
    public Long salary;
    public String department;

}
