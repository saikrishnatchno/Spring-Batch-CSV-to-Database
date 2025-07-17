package org.example.batch;

import org.example.entity.EmployeeData;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class EmployeeItemProcessor implements ItemProcessor <EmployeeData ,EmployeeData>{

    @Override
    public EmployeeData process(EmployeeData item) throws Exception {

        return item;
    }
}
