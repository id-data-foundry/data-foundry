package services.api;

import java.time.Duration;

import javax.inject.Singleton;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;

@Singleton
public class ThrottlingService {

    private final Cache<String, Bucket> cache = Caffeine.newBuilder()
            .maximumSize(10_000)
            .expireAfterAccess(Duration.ofMinutes(15))
            .build();

    public Bucket resolveBucket(String apiKey) {
        return cache.get(apiKey, this::newBucket);
    }

    private Bucket newBucket(String apiKey) {
        // 1 request per second refill rate
        Refill refill = Refill.intervally(1, Duration.ofSeconds(1));
        // Burst capacity of 20
        Bandwidth limit = Bandwidth.classic(20, refill);
        return Bucket.builder()
            .addLimit(limit)
            .build();
    }

    public boolean tryConsume(String apiKey) {
        Bucket bucket = resolveBucket(apiKey);
        return bucket.tryConsume(1);
    }
}
