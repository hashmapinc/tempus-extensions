package com.hashmapinc.tempus;

import com.hashmapinc.tempus.CompactionProtos.CompactedData;
import com.hashmapinc.tempus.CompactionProtos.CompactionRequest;
import com.hashmapinc.tempus.CompactionProtos.CompactionResponse;
import com.hashmapinc.tempus.CompactionProtos.CompactionService;
import com.hashmapinc.tempus.codec.ValueCodec;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public interface AsyncInterface {

    Logger log = Logger.getLogger(AsyncInterface.class);

    static CompletableFuture<List<TagList>> getCompactionTagList(ExecutorService executor,
                                                                 DatabaseService dbService) {
        int NUM_RETRIES_CONNECTING_TO_DATABASE = 5;
        int DEFAULT_RETRY_MILLIS = 10000;
        CompletableFuture<List<TagList>> future = CompletableFuture.supplyAsync(() -> {
            List<TagList> tagList = dbService.getDistinctURI(NUM_RETRIES_CONNECTING_TO_DATABASE, DEFAULT_RETRY_MILLIS);
            return tagList;
        }, executor);
        return future;
    }

    static CompletableFuture<List<CompletableFuture<List<Map<Long, Long[]>>>>>
    compactAllUriPartitions
            (ExecutorService executor, List<List<TagList>> listListUri, DatabaseService dbService,
             long
                    startTs, long endTs, Integer compactionWindowTimeInSecs, final Table table) {
        List<CompletableFuture<CompletableFuture<List<Map<Long, Long[]>>>>> allMinMaxTs = listListUri.stream()
                .map(ptList -> compactAsync(executor, ptList, dbService, startTs, endTs, compactionWindowTimeInSecs, table))
                .collect(Collectors.toList());

        return CompletableFuture.allOf(allMinMaxTs.toArray(new CompletableFuture[allMinMaxTs.size()]))
                .thenApply(q -> {
                    return allMinMaxTs.stream()
                            .map(eachFuture -> eachFuture.join())
                            .collect(Collectors.toList());
                }).handle((results, ex) -> {
                    if (results != null) {
                        return results;
                    } else {
                        log.info("Error while compactAllUriPartitions" + ex.toString());
                        throw new RuntimeException("Error while compactAllUriPartitions" + ex.toString());
                    }
                });
    }

    static CompletableFuture<CompletableFuture<List<Map<Long, Long[]>>>>
    compactAsync(ExecutorService executor, final List<TagList> uris, DatabaseService dbService, long
            startTs, long endTs, Integer compactionWindowTimeInSecs, final Table table) {

        CompletableFuture<CompletableFuture<List<Map<Long, Long[]>>>> future = CompletableFuture
                .supplyAsync(() -> {
                    Map<Long, String> uriDataTypeMap = uris.stream()
                            .collect(Collectors.toMap(TagList::getId, TagList::getDataType));

                    if (log.isDebugEnabled()) {
                        uriDataTypeMap.forEach((uri, dataType) -> {
                            log.info("Uri: " + uri + "; Datatype: " + dataType);
                        });
                    }
                    log.info("Calling getMinMaxTs & compactAndUpsert for uris ");
                    if (log.isTraceEnabled()) {
                        log.info("URI's: " + (uris.stream().map(tl -> tl.getId()).collect
                                (Collectors.toList())).toString());
                    }
                    CompletableFuture<List<TagData>> allMinMaxTs = getMinMaxTs(executor,
                            uris, dbService);

                    return allMinMaxTs.thenCompose((List<TagData> listTagData) -> {
                        listTagData = listTagData.stream()
                                .filter(Objects::nonNull)
                                .filter(td -> td.getUri() != 0)
                                .filter(td -> td.getMinTs() != null)
                                .filter(td -> td.getMaxTs() != null)
                                .collect(Collectors.toList());


                        List<CompletableFuture<Map<Long, Long[]>>> futureCptdList = new
                                ArrayList<>();

                        for (TagData td : listTagData) {
                            futureCptdList.add(compactAndUpsert(executor, td, dbService, startTs,
                                    endTs, compactionWindowTimeInSecs, table, uriDataTypeMap.get
                                            (td.getUri())));
                        }

                        return CompletableFuture.allOf(futureCptdList.toArray(new
                                CompletableFuture[futureCptdList.size()]))
                                .thenApply(q -> {
                                    return futureCptdList.stream()
                                            .map(eachFuture -> eachFuture.join())
                                            .collect(Collectors.toList());
                                });
                    }).handle((results, ex) -> {
                        if (results != null) {
                            return results;
                        } else {
                            log.info("Error while compactAsync" + ex.toString());
                            throw new IllegalStateException("Error while compactAsync" + ex.toString());
                        }
                    });
                }, executor);
        return future;
    }

    static CompletableFuture<List<TagData>> getMinMaxTs(final Executor executor, final
    List<TagList> uris, final DatabaseService dbService) {

        List<CompletableFuture<TagData>> futureTSDetails = uris.stream()
                .map(uri -> getMinMaxTsAsync(executor, uri.getId(), dbService))
                .collect(Collectors.toList());

        return CompletableFuture.allOf(futureTSDetails.toArray(new CompletableFuture[futureTSDetails.size()]))
                .thenApply(v -> {
                    return futureTSDetails.stream()
                            .map(tdTSFuture -> tdTSFuture.join())
                            .collect(Collectors.toList());
                }).handle((results, ex) -> {
                    if (results != null) {
                        return results;
                    } else {
                        log.info("Error while getMinMaxTs" + ex.toString());
                        throw new RuntimeException("Error while getMinMaxTs" + ex.toString());
                    }
                });
    }

    static CompletableFuture<TagData> getMinMaxTsAsync(Executor executor, final long uri,
                                                       final DatabaseService dbs) {
        CompletableFuture<TagData> future = CompletableFuture.supplyAsync(() -> {
            synchronized (dbs) {
                TagData ptTsDetails = null;
                try {
                    log.info("Get min max ts for uri: " + uri);
                    ptTsDetails = dbs.getMinMaxTs(uri);
                } catch (SQLException e) {
                    log.info("For " + uri + " Got SQLException: " + e.getSQLState() + "; " + e.getLocalizedMessage());
                    throw new IllegalStateException(e);
                }
                return ptTsDetails;
            }
        }, executor);
        return future;
    }

    static CompletableFuture<Map<Long, Long[]>>
    compactAndUpsert(Executor executor, final TagData td, final DatabaseService dbs, final long
            startTs, final long endTs, final long windowSecs, final Table table, final
                     String dataType) {
        Map<Long, List<RpcCalls>> rpcListMap = createCompactionRequests(td.getUri(), startTs,
                endTs, dataType, td, windowSecs, dbs).entrySet().stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));

        long numOfRpcCalls = rpcListMap.keySet().stream().findFirst().get();
        List<RpcCalls> listRpcRequests = rpcListMap.get(numOfRpcCalls);


        log.info("Starting Compaction run for URI [" + td.getUri() + "]from[" + (new Timestamp
                (startTs)).toString() + " - " + (new Timestamp(endTs)).toString() + "];");
        List<CompletableFuture<Map<byte[], CompactionResponse>>> futureTdc = listRpcRequests.stream()
                .filter(Objects::nonNull)
                .map(rpcCall -> compactUri(executor, dbs, rpcCall.getStartKey(), rpcCall
                        .getEndKey(), rpcCall.getRequest(), table))
                .collect(Collectors.toList());


        CompletableFuture<Map<Long, Long[]>> futureCompactionStats =
                CompletableFuture.allOf(futureTdc.toArray(new CompletableFuture[futureTdc.size()]))
                        .thenApply(v -> {
                            return futureTdc.stream()
                                    .map(tdcFuture -> tdcFuture.join())
                                    .collect(Collectors.toList());
                        }).thenCompose((List<Map<byte[], CompactionResponse>> listRegionResponse) -> {
                    log.info("Done Compaction run for URI [" + td.getUri() + "]from[" +
                            (new Timestamp(startTs)).toString() + " - " + (new Timestamp(endTs)).toString() + "];");
                    return getCoProcessorResponse(executor, td, listRegionResponse, dbs).thenCompose
                            (mapCompletableFuture -> {
                                Long compactedUri = mapCompletableFuture.keySet().stream().findFirst().get();
                                Long recordsCompacted = mapCompletableFuture.get(compactedUri).keySet().stream().findFirst().get();
                                List<TagDataCompressed> tdcList = (mapCompletableFuture.get(compactedUri)).get(recordsCompacted);
                                return upsertCompactedRecords(executor, tdcList, dbs).thenApply(recordsUpserted -> {
                                    Map<Long, Long[]> finalMap = new HashMap<>();
                                    Long[] longArray = new Long[2];
                                    longArray[0] = recordsCompacted;
                                    longArray[1] = recordsUpserted;
                                    finalMap.put(compactedUri, longArray);
                                    return finalMap;
                                });
                            });
                }).handle((results, ex) -> {
                    if (results != null) {
                        return results;
                    } else {
                        log.info("Error while compactAndUpsert" + ex.toString());
                        throw new RuntimeException("Error while compactAndUpsert" + ex.toString());
                    }
                });

        return futureCompactionStats;
    }

    static Map<Long, List<RpcCalls>> createCompactionRequests(final Long uri,
                                                              final long startTs, final long
                                                                      endTs, final String
                                                                      dataType, final TagData pt,
                                                              final long windowSecs, final DatabaseService dbs) {
        long ONE_SEC_IN_MILLIS = 1000L;
        long windowStartTs = (startTs == 0) ? pt.getMinTs().getTime() : startTs;
        long windowEndTs = windowStartTs + (windowSecs * ONE_SEC_IN_MILLIS);

        String logStartTs = new Timestamp(windowStartTs).toString();
        String logEndTs = new Timestamp(endTs).toString();

        if (uri <= 0) {
            log.error("URI [" + uri + "] can't be compacted");
            return null;
        }

        if (windowStartTs > endTs) {
            log.info(
                    "No Compaction run for [" + uri + "] as windowStartTs i.e. [pt(min(TS))] > " +
                            "endTs.["
                            + logStartTs + " > " + new Timestamp(endTs).toString() + "];");
            return null;
        }
        Map<Long, List<RpcCalls>> requestMap = new HashMap<>();
        long numRequests = 0;
        List<RpcCalls> rpcCallsList = new ArrayList<>();
        while ((endTs - windowStartTs) > 0) {
            String logWindowStartTs = new Timestamp(windowStartTs).toString();
            String logWindowEndTs = new Timestamp(windowEndTs).toString();
            log.debug("Creating compaction request for uri [" + uri + "]; time window[" +
                    logWindowStartTs + " - " + logWindowEndTs + "];");
            byte[] uriBytes = PDataType.fromSqlTypeName("BIGINT").toBytes(uri);
            byte[] startTsBytes = PLong.INSTANCE.toBytes(windowStartTs);
            byte[] endTsBytes = PLong.INSTANCE.toBytes(windowEndTs);
            final byte[] startKey = Bytes.add(uriBytes, startTsBytes);
            final byte[] endKey = Bytes.add(uriBytes, endTsBytes);
            final CompactionRequest rpcRequest = createRpcRequest(uri, windowStartTs, windowEndTs, dataType);
            rpcCallsList.add(new RpcCalls(startKey, endKey, rpcRequest));
            ++numRequests;
            if (windowEndTs > pt.getMaxTs().getTime()) {
                break;
            }
            windowStartTs = windowEndTs;
            windowEndTs = windowStartTs + (windowSecs * ONE_SEC_IN_MILLIS);
        }
        requestMap.put(numRequests, rpcCallsList);
        return requestMap;
    }

    static CompactionRequest createRpcRequest(final long uri,
                                              final long startTs, final
                                              long endTs, final String dataType) {
        CompactionRequest.Builder requestBuilder = CompactionRequest.newBuilder();
        return requestBuilder.setUri(uri).setStartTime(startTs).setEndTime(endTs).setDataType
                (dataType).build();
    }

    static CompletableFuture<Map<byte[], CompactionResponse>>
    compactUri(Executor executor, final DatabaseService dbs, final byte[] startKey, final
    byte[] endKey, final CompactionRequest request, Table table) {

        CompletableFuture<Map<byte[], CompactionResponse>> future = CompletableFuture.supplyAsync(() -> {
            synchronized (dbs) {
                if (log.isDebugEnabled())
                    log.info("Starting Compaction run for [" + request.getUri() + "]from[" +
                            (new Timestamp(request.getStartTime())).toString() + " - " + (new Timestamp(request.getEndTime())).toString() + "];");
                Map<byte[], CompactionResponse> results = null;
                try {
                    results = table.coprocessorService(CompactionService.class, startKey, endKey,
                            new Batch.Call<CompactionService, CompactionResponse>() {
                                @Override
                                public CompactionResponse call(CompactionService aggregate) throws IOException {
                                    BlockingRpcCallback<CompactionResponse> rpcCallback =
                                            new BlockingRpcCallback<CompactionResponse>();
                                    aggregate.compactData(null, request, rpcCallback);
                                    CompactionResponse response = (CompactionResponse) rpcCallback.get();
                                    return response;
                                }
                            });
                } catch (Throwable throwable) {
                    log.error("Got exception in table.coprocessorService() for : " + throwable
                            .getMessage() + "[" + request.getUri() + "]from[" +
                            (new Timestamp(request.getStartTime())).toString() + " - " + (new Timestamp(request.getEndTime())).toString() + "];");
                    // Got an exception for one time-window means the entire data shouldn't be
                    // compacted for now. Thrown an error from here
                    throw new IllegalStateException(throwable.getMessage());
                }
                if (log.isDebugEnabled())
                    log.info("Done Compaction run for time window [" + request.getUri() + "]from[" +
                            (new Timestamp(request.getStartTime())).toString() + " - " + (new Timestamp(request.getEndTime())).toString() + "];");
                return results;
            }
        }, executor);
        return future;
    }

    static CompletableFuture<Map<Long, Map<Long, List<TagDataCompressed>>>>
    getCoProcessorResponse(final Executor executor, final TagData td, final List<Map<byte[],
            CompactionResponse>> listRegionResponse, final DatabaseService dbs) {
        CompletableFuture<Map<Long, Map<Long, List<TagDataCompressed>>>> future = CompletableFuture.supplyAsync(() -> {
            List<TagDataCompressed> tdcList = new ArrayList<>();
            long numCompacted = 0;

            Map<Long, Map<Long, List<TagDataCompressed>>> responseMap = new HashMap<>();
            Map<Long, List<TagDataCompressed>> tdcMap = new HashMap<>();
            for (Map<byte[], CompactionResponse> compactionResp : listRegionResponse) {
                Long compactedUri = td.getUri();
                if (compactionResp != null) {
                    if (log.isTraceEnabled()) {
                        log.trace("PB response:[" + compactionResp.toString() + "]; Size:[" +
                                compactionResp.size() + "]; Key:[" + compactionResp.keySet().toString() + "]");
                    }
                    for (CompactionResponse response : compactionResp.values()) {
                        if (response.hasPackedData()) {
                            TagDataCompressed tdc = new TagDataCompressed();
                            CompactedData compactData = response.getPackedData();
                            if (compactedUri.equals(compactData.getUri())) {
                                tdc.setId(compactData.getUri());
                                tdc.setStTs(new Timestamp(compactData.getFirstptTs()));
                                tdc.setTs(compactData.getTs().toByteArray());
                                tdc.setQ(compactData.getQuality().toByteArray());
                                tdc.setVb(compactData.getVb().toByteArray());
                                tdc.setNs(compactData.getNumSamples());
                                if (log.isTraceEnabled()) {
                                    log.debug("TDC = " + tdc.toString());
                                }
                                tdcList.add(tdc);
                                numCompacted += compactData.getNumSamples();
                            } else {
                                log.error("Compaction request was made for URI: " + compactedUri
                                        + " but recvd response for URI: " + compactData.getUri());
                                throw new IllegalStateException("Compaction request was made for " +
                                        "URI: " + compactedUri + " but recvd response for URI: " + compactData
                                        .getUri());
                            }
                        } else {
                            // In case of no results present for time window we set isFail to false
                            // Also error_msg is set with NO_ERR:....
                            if (response.hasErrMsg() && response.getErrMsg().startsWith("NO_ERR")) {
                                if (log.isDebugEnabled())
                                    log.info("URI:[" + td.getUri() + "]; Resp msg:- " + response.getErrMsg());
                            } else {
                                // Coprocessor recvd some exception while processing data.
                                // isFail will be true and exception will be in response.getErrMsg()
                                if (response.getIsFail()) {
                                    log.error("URI:[" + td.getUri() + "]; Received exception with error messsage " + response.getErrMsg());
                                    throw new IllegalStateException(response.getErrMsg());
                                } else {
                                    // Should never come here as we have handled failures in exception
                                    // If we reach here throw an exception ???
                                    // This block happens when ERR(no_packed_data) && ERR(no_fail) && ERR(no_err_msg)
                                    // For now, lets log(ERROR) and continue to the next compaction window for the TD
                                    log.error(
                                            "URI:[" + td.getUri() + "]; Unhandled state  ERR(no_packed_data) && ERR(no_fail) && ERR(no_err_msg).");
                                    // throw new IllegalStateException("E(no_packed_data) && E(no_fail) &&
                                    // E(no_err_msg)");
                                }
                            }
                        }
                    }
                } else {
                    log.info("URI:[" + td.getUri() + "]; Check RS logs. Compaction Service returned null response for TimeWindow.:"
                            /*+ windowStartTs + " - " + windowEndTs*/);
                    throw new IllegalStateException("URI:[" + td.getUri() + "]; Check RS logs. Compaction Service returned null response for TimeWindow.:");
                }
            }
            tdcMap.put(numCompacted, tdcList);
            responseMap.put(td.getUri(), tdcMap);
            return responseMap;
        }, executor);
        return future;
    }

    static CompletableFuture<Long> upsertCompactedRecords(Executor executor, final List<TagDataCompressed> compactedRecords, final DatabaseService dbs) {
        CompletableFuture<Long> future = CompletableFuture.supplyAsync(() -> {
            synchronized (dbs) {
                long recordsUpserted = 0;
                try {
                    recordsUpserted = dbs.upsertCompactedRecords(compactedRecords);
                } catch (Exception e) {
                    throw new IllegalStateException(e.getMessage());
                }
                return recordsUpserted;
            }
        }, executor);
        return future;
    }

    static CompletableFuture<List<Long>> deleteCompactedRecords(Executor executor, List<List<Long>> deleteLists, final DatabaseService dbs, final long startTs, final long endTs) {
        List<CompletableFuture<Long>> futureDeletedRecords = deleteLists.stream()
                .map(deleteList -> deleteCompactedUris(executor, deleteList, dbs, startTs, endTs))
                .collect(Collectors.toList());

        CompletableFuture<List<Long>> numDeletedRecords = CompletableFuture.allOf(futureDeletedRecords.toArray(new CompletableFuture[futureDeletedRecords.size()])).thenApply(v -> {
            return futureDeletedRecords.stream()
                    .map(deletedFuture -> deletedFuture.join())
                    .collect(Collectors.toList());
        }).handle((results, ex) -> {
            if (results != null) {
                return results;
            } else {
                log.info("Error while deleteCompactedRecords" + ex.toString());
                throw new RuntimeException("Error while deleteCompactedRecords" + ex.toString());
            }
        });
        return numDeletedRecords;
    }

    static CompletableFuture<Long> deleteCompactedUris(Executor executor, final List<Long> uris,
                                                     final DatabaseService dbs, final long startTs, final long endTs) {

        if (log.isDebugEnabled()) {
            log.info("Calling Deletes on list size of " + uris.size());
            log.info("Calling deletes on URI's: " + uris.toString());
        }
        CompletableFuture<Long> future = CompletableFuture.supplyAsync(() -> {
            synchronized (dbs) {
                long deletedRecords = 0;
                try {
                    deletedRecords = dbs.deleteCompactedURIs(uris, startTs, endTs);
                } catch (SQLException e) {
                    log.info("For [" + uris.toString() + "] Got SQLException: " + e.getSQLState() +
                            "; " + e.getLocalizedMessage());
                    throw new IllegalStateException("For [" + uris.toString() + "] Got SQLException: " + e.getSQLState() +
                            "; " + e.getLocalizedMessage());
                }
                return deletedRecords;
            }
        }, executor);
        return future;
    }
}