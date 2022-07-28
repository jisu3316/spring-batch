package study.spring.batch.springbatchstudy.part2;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class SharedConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    /**
     * start는 시작  그 다음 next 순차적으로 실행된다.
     * @return
     */
    @Bean
    public Job shareJob() {
        return jobBuilderFactory.get("shareJob")
                .incrementer(new RunIdIncrementer())
                .start(this.shareStep())
                .next(this.shareStep2())
                .build();
    }

    /**
     * contribution 을 이용해서 stepExecution 객체를 꺼낼수 있다.
     * stepExecution을 이용해서 stepExecutionContext을 꺼낼수 있다.
     * @return
     */
    @Bean
    public Step shareStep() {
        return stepBuilderFactory.get("shareStep1")
                .tasklet((contribution, chunkContext) -> {
                    // step ExecutionContext.put
                    StepExecution stepExecution = contribution.getStepExecution();
                    ExecutionContext stepExecutionContext = stepExecution.getExecutionContext();
                    stepExecutionContext.putString("step", "step execution context");

                    // job ExecutionContext.put
                    JobExecution jobExecution = stepExecution.getJobExecution();
                    ExecutionContext jobExecutionContext = jobExecution.getExecutionContext();
                    jobExecutionContext.putString("job", "job execution context");

                    // log
                    JobInstance jobInstance = jobExecution.getJobInstance();
                    JobParameters jobParameters = jobExecution.getJobParameters();
//                    JobParameters jobParameters1 = stepExecution.getJobParameters();

                    log.info("jobName : {}, stepName : {}, run.id : {}",
                            jobInstance.getJobName(),
                            stepExecution.getStepName(),
                            jobParameters.getLong("run.id"));

                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step shareStep2() {
        return stepBuilderFactory.get("shareStep2")
                .tasklet((contribution, chunkContext) -> {
                    // step ExecutionContext.get
                    StepExecution stepExecution = contribution.getStepExecution();
                    ExecutionContext stepExecutionContext = stepExecution.getExecutionContext();

                    // job ExecutionContext.get
                    JobExecution jobExecution = stepExecution.getJobExecution();
                    ExecutionContext jobExecutionContext = jobExecution.getExecutionContext();

                    StepExecution stepExecution2 = contribution.getStepExecution();
                    ExecutionContext stepExecutionContext2 = stepExecution2.getExecutionContext();
                    stepExecutionContext2.putString("step", "step execution context");

                    // log
                    log.info("jobValue : {}, stepValue : {}",
                            jobExecutionContext.getString("job", "emptyJob"),
                            stepExecutionContext.getString("step", "emptyStep"));

                    return RepeatStatus.FINISHED;

                }).build();
    }
}
