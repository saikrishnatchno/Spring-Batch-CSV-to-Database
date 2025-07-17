package org.example.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EmployeeJobController {


    @Autowired
    public JobLauncher jobLauncher;

    @Autowired
    private Job csvTODbJob;

    /**
     * REST endpoint to trigger the CSV to Database import batch job.
     * <p>
     * URL: POST /import
     * <p>
     * This method builds unique JobParameters using the current timestamp to ensure
     * the job instance is identifiable and can run multiple times.
     * It invokes the Spring Batch Job configured to read employee data from a CSV file,
     * process it, and store it in the database.
     */
    @PostMapping("/import")
    public void loadCustomerData() throws Exception {

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis()).toJobParameters();

        try {
            jobLauncher.run(csvTODbJob, jobParameters);
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException |
                 JobParametersInvalidException e) {
            throw new RuntimeException(e);

        }
    }
}
