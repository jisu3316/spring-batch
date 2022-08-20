package study.spring.batch.springbatchstudy.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.EntityManagerFactory;

/**
 * 요구 사항 csv 파일 데이터를 읽어 H2 DB에 데이터 저장하는 배치 개발
 * Reader
 * 100개의 person data를 csv 파일에서 읽는다.
 *
 * Processor
 *      allow_duplicate 파라미터로 person.name의 중복 여부 조건을 판단한다.
 *      `allow_duplicate=true` 인 경우 모든 person을 return 한다.
 *      `allow_duplicate=false 또는 null` 인 경우 person.name이 중복된 데이터는 null로 return 한다.
 *      힌트 : 중복 체크는 `java.util.Map` 사용
 * Writer
 *      2개의 ItemWriter를 사용해서 Person H2 DB에 저장 후 몇 건 저장됐는 지 log를 찍는다.
 *      Person 저장 ItemWriter와 log 출력 ItemWriter
 * 힌트 : `CompositeItemWriter` 사용
 */
@Configuration
@Slf4j
@RequiredArgsConstructor
public class SavePersonConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;

    @Bean
    public Job savePersonJob() throws Exception {
        return this.jobBuilderFactory.get("savePersonJob")
                .incrementer(new RunIdIncrementer())
                .start(this.savePersonStep(null))
                .build();
    }

    @Bean
    @JobScope
    public Step savePersonStep(@Value("#{jobParameters[allow_duplicate]}") String allowDuplicate) throws Exception {
        return this.stepBuilderFactory.get("savePersonStep")
                .<Person, Person>chunk(10)
                .reader(itemReader())
                .processor(new DuplicateValidationProcessor<>(Person::getName, Boolean.parseBoolean(allowDuplicate)))
                .writer(itemWrite())
                .build();
    }

    private ItemWriter<? super Person> itemWrite() throws Exception {
//        return items -> items.forEach(x -> log.info("저는 {} 입니다.", x.getName()));
        JpaItemWriter<Person> jpaItemWriter = new JpaItemWriterBuilder<Person>()
                .entityManagerFactory(entityManagerFactory)
                .build();

        ItemWriter<Person> logItemWriter = items -> log.info("person.size {}", items.size());

        //delegates로 추가한 itemWriter는 추가한 순서대로 실행된다.
        CompositeItemWriter<Person> itemWriter = new CompositeItemWriterBuilder<Person>()
                .delegates(jpaItemWriter, logItemWriter)
                .build();

        itemWriter.afterPropertiesSet();
        return itemWriter;
    }

    private ItemReader<? extends Person> itemReader() throws Exception {
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames("name", "age", "address");
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSet -> new Person(
                fieldSet.readString(0),
                fieldSet.readString(1),
                fieldSet.readString(2)));

        FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
                .name("csvFileItemReader")
                .encoding("UTF-8")
                .resource(new ClassPathResource("person.csv")) //ClassPathResource spring에서 제공하는 클래스 resources 밑의 파일을 읽을수있다.
                .linesToSkip(1)  //test.csv를 가면 첫번째라인은 필드명을 정의해놓은거라 두번쨰 라인부터 읽겠다는 의미
                .lineMapper(lineMapper)
                .build();
        itemReader.afterPropertiesSet();
        return itemReader;
    }
}
