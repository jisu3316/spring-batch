package study.spring.batch.springbatchstudy.part3;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.util.ArrayList;
import java.util.List;

/**
 * 리시트를 생성자로 주입을 받았고 엘리먼트를 하나씩제거하고 리턴하는 클래스이다.
 * 만약 없으면 널은 리턴한다 널의 의미는 정크 반복이 끝났다.
 */

@RequiredArgsConstructor
public class CustomItemReader<T> implements ItemReader<T> {

    private final List<T> items;

    @Override
    public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (!items.isEmpty()) {
            return items.remove(0);
        }

        return null;
    }
}
