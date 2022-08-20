package study.spring.batch.springbatchstudy.part3;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;


@RunWith(SpringRunner.class)
@SpringBatchTest
@ContextConfiguration(classes = {SavePersonConfiguration.class, TestConfiguration.class})
class SavePersonConfigurationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    private PersonRepository personRepository;

    @AfterEach
    public void tearDown() throws Exception {
        personRepository.deleteAll();
    }

    @Test
    public void test_step() {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("savePersonStep");//파라미터를 넘길 수 있다.

        Assertions.assertThat(jobExecution.getStepExecutions().stream()
                        .mapToInt(StepExecution::getWriteCount)
                        .sum())
                .isEqualTo(personRepository.count())
                .isEqualTo(3);
    }

    @Test
    public void test_allow_duplicate() throws Exception {
        //given
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("allow_duplicate", "false")
                .toJobParameters();

        //when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        //SavePersonConfiguration에서 잡이 launchJob에서 실행이 된다. 실행된 잡의 결과를 jobExecution으로 받는다.

        //then
        Assertions.assertThat(jobExecution.getStepExecutions().stream()
                .mapToInt(StepExecution::getWriteCount)
                .sum())
                .isEqualTo(personRepository.count())
                .isEqualTo(3);
    }

    @Test
    public void test_not_allow_duplicate() throws Exception {
        //given
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("allow_duplicate", "true")
                .toJobParameters();

        //when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        //SavePersonConfiguration에서 잡이 launchJob에서 실행이 된다. 실행된 잡의 결과를 jobExecution으로 받는다.

        //then
        Assertions.assertThat(jobExecution.getStepExecutions().stream()
                        .mapToInt(StepExecution::getWriteCount)
                        .sum())
                .isEqualTo(personRepository.count())
                .isEqualTo(100);
    }
}