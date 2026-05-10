package services.api;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Singleton;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;

@Singleton
public class ThrottlingService {

    private final ConcurrentHashMap<String, Bucket> cache = new ConcurrentHashMap<>();

    public Bucket resolveBucket(String apiKey) {
        return cache.computeIfAbsent(apiKey, this::newBucket);
    }

    private Bucket newBucket(String apiKey) {
        // 60 requests per minute per token
        Refill refill = Refill.intervally(60, Duration.ofMinutes(1));
        Bandwidth limit = Bandwidth.classic(60, refill);
        return Bucket.builder()
            .addLimit(limit)
            .build();
    }

    public boolean tryConsume(String apiKey) {
        Bucket bucket = resolveBucket(apiKey);
        return bucket.tryConsume(1);
    }
}
