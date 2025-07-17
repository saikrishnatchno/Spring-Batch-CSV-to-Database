package org.example.batch;

import org.example.entity.EmployeeData;
import org.example.repository.EmployeeRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private EmployeeRepository employeeRepository;

    /**
     * Configures an ItemReader to read EmployeeData from a CSV file.
     *
     * @return FlatFileItemReader that reads and maps lines from the 'customer_data.csv' file into EmployeeData objects.
     */
    @Bean
    public FlatFileItemReader<EmployeeData> readEmpData(){
        FlatFileItemReader<EmployeeData> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customer_data.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }


    /**
     * Creates a LineMapper to map CSV lines to EmployeeData objects.
     * Uses a DelimitedLineTokenizer to parse CSV lines and a BeanWrapperFieldSetMapper to map fields.
     *
     * @return LineMapper<EmployeeData> for mapping CSV data into EmployeeData instances.
     */
    private LineMapper<EmployeeData> lineMapper() {
        DefaultLineMapper<EmployeeData> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames( "firstName", "lastName", "email", "contact", "country", "dob");

        BeanWrapperFieldSetMapper<EmployeeData> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(EmployeeData.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;

    }


    /**
     * Defines the item processor to process each EmployeeData record after reading.
     */
    @Bean
    public EmployeeItemProcessor employeeItemProcessor(){
        return  new EmployeeItemProcessor();
    }


    /**
     * Configures the ItemWriter to persist EmployeeData entities into the database
     * using the EmployeeRepository.
     *
     * @return RepositoryItemWriter<EmployeeData> for writing processed data to the database.
     */
    @Bean
    public RepositoryItemWriter<EmployeeData> employeeWriter(){
        RepositoryItemWriter<EmployeeData> writer = new RepositoryItemWriter<>();
        writer.setRepository(employeeRepository);
        writer.setMethodName("save");
        return writer;
    }

    /**
     * Defines a Spring Batch Step to read from CSV, process data, and write to the database.
     */
    @Bean
    public Step csvToDbStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("csv-to-db-step", jobRepository)
                .<EmployeeData, EmployeeData>chunk(20, transactionManager)
                .reader(readEmpData())
                .processor(employeeItemProcessor())
                .writer(employeeWriter())
                .build();
    }

    /**
     * Configures a Spring Batch Job that runs the CSV to DB step.
     */
    @Bean
    public Job csvTODbJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("csv-import-job", jobRepository)
                .flow(csvToDbStep(jobRepository,transactionManager))
                .end()
                .build();
    }


    /**
     * Defines a TaskExecutor for asynchronous task execution within the batch job.
     * Sets concurrency limit to 10 for parallel processing.
     *
     * @return TaskExecutor configured for async processing.
     */
    @Bean
    public TaskExecutor taskExecutor(){
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }


}
