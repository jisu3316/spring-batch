package study.spring.batch.springbatchstudy.part3;

import org.springframework.batch.item.ItemProcessor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DuplicateValidationProcessor<T> implements ItemProcessor<T, T> {

    private final Map<String, Object> keyPool = new ConcurrentHashMap<>();
    private final Function<T, String> keyExtractor;
    private final boolean allowDuplicate;

    public DuplicateValidationProcessor(Function<T, String> keyExtractor, boolean allowDuplicate) {
        this.keyExtractor = keyExtractor;
        this.allowDuplicate = allowDuplicate;
    }

    @Override
    public T process(T item) throws Exception {
        // 트루면 필터링(중복체크)를 하지 않겠다는 의미해서 아이템을 리턴한다.
        if (allowDuplicate) {
            return item;
        }

        //item에서 key를 추출한다.
        String key = keyExtractor.apply(item);

        if (keyPool.containsKey(key)) {
            return null;
        }

        keyPool.put(key, key);

        return item;
    }
}
