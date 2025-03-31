
package org.supreme.models;

import lombok.Data;

@Data
public class DepartMentWiseSalary {

    public String department;
    public long totalSalary;
    public long totalEmployee;
    public long avgSalary;
    
}