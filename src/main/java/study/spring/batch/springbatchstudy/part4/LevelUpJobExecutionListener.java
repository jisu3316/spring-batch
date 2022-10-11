package study.spring.batch.springbatchstudy.part4;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.time.LocalDateTime;
import java.util.Collection;

@Slf4j
@RequiredArgsConstructor
public class LevelUpJobExecutionListener implements JobExecutionListener {

    private final UserRepository userRepository;
    @Override
    public void beforeJob(JobExecution jobExecution) {

    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        Collection<Users> users = userRepository.findAllByUpdatedDate(LocalDateTime.now());

        long time = jobExecution.getEndTime().getTime() - jobExecution.getStartTime().getTime();
        log.info("회원등금 업데이트 배치 프로그램");
        log.info("---------------------------");
        log.info("총 데이터 처리 {}건, 처리시간 {}millis", users.size(), time);
    }
}
