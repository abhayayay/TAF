package com.couchbase.test.doc_operations_sdk3;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import reactor.core.scheduler.Schedulers;
import java.util.concurrent.atomic.AtomicInteger;
import org.reactivestreams.Publisher;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.ScanType;
import com.couchbase.client.java.kv.RangeScan;
import com.couchbase.client.java.kv.ScanTerm;
import com.couchbase.client.java.kv.ScanOptions;
import com.couchbase.client.java.kv.SamplingScan;
import com.couchbase.client.java.kv.PrefixScan;
import com.couchbase.client.java.kv.ScanResult;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.kv.TouchOptions;
import com.couchbase.client.java.kv.UpsertOptions;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import java.util.stream.*;

public class doc_ops {

public ConcurrentHashMap<String, Object> sampling_scan(Collection collection, long limit, long seed, ScanOptions options){
    ConcurrentHashMap<String, Object> returnValue = new ConcurrentHashMap<String, Object>();
    returnValue.put("count", 0);
    returnValue.put("timeTaken", 0);
    returnValue.put("exception", "");
    returnValue.put("status", false);
    long startTime = System.nanoTime();
    try {
            SamplingScan samplingScan = ScanType.samplingScan(limit);
            Stream<ScanResult>  documents = collection.scan(samplingScan,options);
            long count =0;
            count = documents.parallel().count();
            long endTime = System.nanoTime();
            long executionTime = (endTime - startTime) / 1000000;
            returnValue.put("count", count);
            returnValue.put("status", true);
            returnValue.put("timeTaken", executionTime);
            return(returnValue);
    }
    catch(Exception e) {
        long endTime = System.nanoTime();
        long executionTime = (endTime - startTime) / 1000000;
        returnValue.put("count", -1);
        returnValue.put("status", false);
        returnValue.put("timeTaken", executionTime);
        returnValue.put("exception", e);
        return(returnValue);
        }
    }


public ConcurrentHashMap<String, Object> prefix_scan_query(Collection collection, String scan_term, ScanOptions options){
    ConcurrentHashMap<String, Object> returnValue = new ConcurrentHashMap<String, Object>();
    returnValue.put("count", 0);
    returnValue.put("timeTaken", 0);
    returnValue.put("exception", "");
    returnValue.put("status", false);
    long startTime = System.nanoTime();
    try {
            PrefixScan prefixScan = ScanType.prefixScan(scan_term);
            Stream<ScanResult>  documents = collection.scan(prefixScan,options);
            long count =0;
            count = documents.parallel().count();
            long endTime = System.nanoTime();
            long executionTime = (endTime - startTime) / 1000000;
            returnValue.put("count", count);
            returnValue.put("status", true);
            returnValue.put("timeTaken", executionTime);
            return(returnValue);
        }
        catch(Exception e) {
            long endTime = System.nanoTime();
            long executionTime
            = (endTime - startTime) / 1000000;
            returnValue.put("count", -1);
            returnValue.put("status", false);
            returnValue.put("timeTaken", executionTime);
            returnValue.put("exception", e);
            return(returnValue);
        }
    }

public ConcurrentHashMap<String, Object> range_scan_query(Collection collection, String startTerm, String endTerm, boolean includeStartTerm ,  boolean includeEndTerm, ScanOptions options){
    ConcurrentHashMap<String, Object> returnValue = new ConcurrentHashMap<String, Object>();
    returnValue.put("count", 0);
    returnValue.put("timeTaken", 0);
    returnValue.put("exception", "");
    returnValue.put("status", false);
    long startTime = System.nanoTime();
    try {
            ScanTerm searchStartTerm = includeStartTerm == true ? ScanTerm.inclusive(startTerm): ScanTerm.exclusive(startTerm);
            ScanTerm searchEndTerm = includeEndTerm == true ? ScanTerm.inclusive(endTerm): ScanTerm.exclusive(endTerm);
            RangeScan rangeScan = ScanType.rangeScan(searchStartTerm, searchEndTerm);
            Stream<ScanResult>  documents = collection.scan(rangeScan,options);
            long count =0;
            count = documents.parallel().count();
            long endTime = System.nanoTime();
            long executionTime = (endTime - startTime) / 1000000;
            returnValue.put("count", count);
            returnValue.put("status", true);
            returnValue.put("timeTaken", executionTime);
            return(returnValue);
        }
        catch(Exception e) {
            long endTime = System.nanoTime();
            long executionTime = (endTime - startTime) / 1000000;
            returnValue.put("count", -1);
            returnValue.put("status", false);
            returnValue.put("timeTaken", executionTime);
            returnValue.put("exception", e);
            return(returnValue);
        }
    }

public static List<ConcurrentHashMap<String, Object>> bulkInsert_new(Collection collection,
            List<Tuple2<String, Object>> documents,
            InsertOptions insertOptions) {
        ReactiveCollection reactiveCollection = collection.reactive();
        final int numberOfThreads = 25;
        return Flux.fromIterable(documents)
                // Divide the work into numberOfThreads chunks
                .parallel(numberOfThreads)
                // Run on an unlimited thread pool
                .runOn(Schedulers.newBoundedElastic(numberOfThreads, Integer.MAX_VALUE, "bulkInsert"))
                .concatMap(documentToInsert -> {
                    String id = documentToInsert.getT1();
                    Object content = documentToInsert.getT2();
                    final ConcurrentHashMap<String, Object> retValue = new ConcurrentHashMap<>();
                    retValue.put("document", content);
                    retValue.put("error", "");
                    retValue.put("cas", 0);
                    retValue.put("status", true);
                    retValue.put("id", id);
                    return reactiveCollection.insert(id, content, insertOptions)
                            .map(result -> {
                                retValue.put("result", result);
                                retValue.put("cas", result.cas());
                                return retValue;
                            }).onErrorResume(error -> {
                                retValue.put("error", error);
                                retValue.put("status", false);
                                return Mono.just(retValue);
                            });
                }).sequential().collectList().block();
    }
    public List<ConcurrentHashMap<String, Object>> bulkInsert(Collection collection, List<Tuple2<String, Object>> documents,
            InsertOptions insertOptions) {
        ReactiveCollection reactiveCollection = collection.reactive();
        List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(documents)
                .flatMap(new Function<Tuple2<String, Object>, Publisher<ConcurrentHashMap<String, Object>>>() {
                    public Publisher<ConcurrentHashMap<String, Object>> apply(Tuple2<String, Object> documentToInsert) {
                        String id = documentToInsert.getT1();
                        Object content = documentToInsert.getT2();
                        final ConcurrentHashMap<String, Object> retValue = new ConcurrentHashMap<String, Object>();
                        retValue.put("document", content);
                        retValue.put("error", "");
                        retValue.put("cas", 0);
                        retValue.put("status", true);
                        retValue.put("id", id);
                        return reactiveCollection.insert(id, content, insertOptions)
                                .map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
                                    public ConcurrentHashMap<String, Object> apply(MutationResult result) {
                                        retValue.put("result", result);
                                        retValue.put("cas", result.cas());
                                        return retValue;
                                    }
                                }).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
                                    public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
                                        retValue.put("error", error);
                                        retValue.put("status", false);
                                        return Mono.just(retValue);
                                    }
                                });
                    }
                }).collectList().block();
        return returnValue;
    }

    public List<ConcurrentHashMap<String, Object>> bulkUpsert(Collection collection, List<Tuple2<String, Object>> documents,
            UpsertOptions upsertOptions) {
        final ReactiveCollection reactiveCollection = collection.reactive();
        List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(documents)
                .flatMap(new Function<Tuple2<String, Object>, Publisher<ConcurrentHashMap<String, Object>>>() {
                    public Publisher<ConcurrentHashMap<String, Object>> apply(Tuple2<String, Object> documentToInsert) {
                        final String id = documentToInsert.getT1();
                        final Object content = documentToInsert.getT2();
                        final ConcurrentHashMap<String, Object> returnValue = new ConcurrentHashMap<String, Object>();
                        returnValue.put("document", content);
                        returnValue.put("error", "");
                        returnValue.put("cas", 0);
                        returnValue.put("status", true);
                        returnValue.put("id", id);
                        return reactiveCollection.upsert(id, content, upsertOptions)
                                .map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
                                    public ConcurrentHashMap<String, Object> apply(MutationResult result) {
                                        returnValue.put("result", result);
                                        returnValue.put("cas", result.cas());
                                        return returnValue;
                                    }
                                }).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
                                    public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
                                        returnValue.put("error", error);
                                        returnValue.put("status", false);
                                        return Mono.just(returnValue);
                                    }
                                });
                    }
                }).collectList().block();
        return returnValue;
    }

    public List<ConcurrentHashMap<String, Object>> bulkReplace(Collection collection, List<Tuple2<String, Object>> documents,
            ReplaceOptions replaceOptions) {
        final ReactiveCollection reactiveCollection = collection.reactive();
        List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(documents)
                .flatMap(new Function<Tuple2<String, Object>, Publisher<ConcurrentHashMap<String, Object>>>() {
                    public Publisher<ConcurrentHashMap<String, Object>> apply(Tuple2<String, Object> documentToInsert) {
                        final String id = documentToInsert.getT1();
                        final Object content = documentToInsert.getT2();
                        final ConcurrentHashMap<String, Object> returnValue = new ConcurrentHashMap<String, Object>();
                        returnValue.put("document", content);
                        returnValue.put("error", "");
                        returnValue.put("cas", 0);
                        returnValue.put("status", true);
                        returnValue.put("id", id);
                        return reactiveCollection.replace(id, content, replaceOptions)
                                .map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
                                    public ConcurrentHashMap<String, Object> apply(MutationResult result) {
                                        returnValue.put("result", result);
                                        returnValue.put("cas", result.cas());
                                        return returnValue;
                                    }
                                }).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
                                    public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
                                        returnValue.put("error", error);
                                        returnValue.put("status", false);
                                        return Mono.just(returnValue);
                                    }
                                });
                    }
                }).collectList().block();
        return returnValue;
    }

    public List<ConcurrentHashMap<String, Object>> bulkTouch(Collection collection, List<String> keys, final int exp,
            TouchOptions touchOptions, Duration exp_duration) {
        final ReactiveCollection reactiveCollection = collection.reactive();
        List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(keys)
                .flatMap(new Function<String, Publisher<ConcurrentHashMap<String, Object>>>() {
                    public Publisher<ConcurrentHashMap<String, Object>> apply(String key){
                        final ConcurrentHashMap<String, Object> retVal = new ConcurrentHashMap<String, Object>();
                        retVal.put("id", key);
                        retVal.put("cas", 0);
                        retVal.put("error", "");
                        retVal.put("status", false);
                        return reactiveCollection.touch(key, exp_duration, touchOptions)
                                .map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
                                    public ConcurrentHashMap<String, Object> apply(MutationResult result){
                                        retVal.put("cas", result.cas());
                                        retVal.put("status", true);
                                        return retVal;
                                    }
                                }).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
                                    public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
                                        retVal.put("error", error);
                                        return Mono.just(retVal);
                            }
                                }).defaultIfEmpty(retVal);
                    }
                }).collectList().block();
        return returnValue;
    }

    public List<ConcurrentHashMap<String, Object>> bulkGet(Collection collection, List<String> keys, GetOptions getOptions) {
        final ReactiveCollection reactiveCollection = collection.reactive();
        List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(keys)
                .flatMap(new Function<String, Publisher<ConcurrentHashMap<String, Object>>>() {
                    public Publisher<ConcurrentHashMap<String, Object>> apply(String key) {
                        final ConcurrentHashMap<String, Object> retVal = new ConcurrentHashMap<String, Object>();
                        retVal.put("id", key);
                        retVal.put("cas", 0);
                        retVal.put("content", "");
                        retVal.put("error", "");
                        retVal.put("status", false);
                        return reactiveCollection.get(key, getOptions)
                            .map(new Function<GetResult, ConcurrentHashMap<String, Object>>() {
                                public ConcurrentHashMap<String, Object> apply(GetResult optionalResult) {
                                        retVal.put("cas", optionalResult.cas());
                                        retVal.put("content", optionalResult.contentAsObject());
                                        retVal.put("status", true);
                                    return retVal;
                                }
                        }).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
                            public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
                                retVal.put("error", error);
                                return Mono.just(retVal);
                            }
                        }).defaultIfEmpty(retVal);
                    }
                }).collectList().block();
        return returnValue;
    }

    public List<ConcurrentHashMap<String, Object>> bulkDelete(Collection collection, List<String> keys, RemoveOptions removeOptions) {
        final ReactiveCollection reactiveCollection = collection.reactive();
        List<ConcurrentHashMap<String, Object>> returnValue = Flux.fromIterable(keys)
                .flatMap(new Function<String, Publisher<ConcurrentHashMap<String, Object>>>() {
                    public Publisher<ConcurrentHashMap<String, Object>> apply(String key){
                        final ConcurrentHashMap<String, Object> retVal = new ConcurrentHashMap<String, Object>();
                        retVal.put("id", key);
                        retVal.put("cas", 0);
                        retVal.put("error", "");
                        retVal.put("status", false);
                        return reactiveCollection.remove(key, removeOptions)
                                .map(new Function<MutationResult, ConcurrentHashMap<String, Object>>() {
                                    public ConcurrentHashMap<String, Object> apply(MutationResult result){
                                        retVal.put("cas", result.cas());
                                        retVal.put("status", true);
                                        return retVal;
                                    }
                                }).onErrorResume(new Function<Throwable, Mono<ConcurrentHashMap<String, Object>>>() {
                                    public Mono<ConcurrentHashMap<String, Object>> apply(Throwable error) {
                                        retVal.put("error", error);
                                        return Mono.just(retVal);
                            }
                                }).defaultIfEmpty(retVal);
                    }
                }).collectList().block();
        return returnValue;
    }

}
