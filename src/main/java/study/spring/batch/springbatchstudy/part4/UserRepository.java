package study.spring.batch.springbatchstudy.part4;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.Collection;

public interface UserRepository extends JpaRepository<Users, Long> {
    Collection<Users> findAllByUpdatedDate(LocalDateTime now);

}
