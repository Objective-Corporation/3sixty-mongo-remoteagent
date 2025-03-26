package com.objective.threesixty.agent.mongodb;

/*-
 * %%
 * 3Sixty Remote Agent Example
 * -
 * Copyright (C) 2025 Objective Corporation Limited.
 * -
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * %-
 */

import com.google.protobuf.Timestamp;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.result.InsertOneResult;
import com.objective.threesixty.Document;
import com.objective.threesixty.MetadataType;
import com.objective.threesixty.remoteagent.sdk.agent.RepositoryWriter;
import com.objective.threesixty.remoteagent.sdk.utils.CustomParameters;
import org.apache.commons.codec.binary.Hex;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;

import static com.objective.threesixty.agent.mongodb.MongoDBConstants.*;

@Component
@Scope("singleton")
public class MongoDBWriter implements RepositoryWriter {

    private MongoClient mongoClient;
    private MongoCollection<org.bson.Document> mongoCollection;
    private org.bson.Document insertDoc;
    private GridFSBucket gridFSBucket;

    @Override
    public Mono<Document> writeDocument(Document doc, Map<String, MetadataType> metadata, Flux<DataBuffer> binaries, CustomParameters params) {
        getLogger().debug("==> In writeDocument() ");

        // Retrieve stored parameters
        boolean gridFS = params.get(USE_GRIDFS).getBoolean();
        String uri = params.get(URI).getString();
        String db = params.get(DB).getString();
        String collectionStr = params.get(COLLECTION).getString();

        // Create MongoClient and MongoDatabase
        mongoClient = MongoClients.create(uri);
        MongoDatabase database = mongoClient.getDatabase(db);

        // Check if using GridFS
        if (!gridFS) {
            // Create MongoCollection
            mongoCollection = database.getCollection(collectionStr);

            // GridFS not used, just insert document
            return Mono.fromCallable(() -> insertDocument(doc, metadata))
                    .subscribeOn(Schedulers.boundedElastic())
                    .doOnError(e -> getLogger().error("==> Error processing non-GridFS document: " + doc.getName(), e))
                    .doFinally(signal ->mongoClient.close());
        } else {
            // Create GridFSBucket and MongoCollection
            gridFSBucket = GridFSBuckets.create(database, collectionStr);
            mongoCollection = database.getCollection(collectionStr + FILES_SUFFIX);

            // GridFS used, write content
            return Mono.fromCallable(() -> writeContent(doc, metadata, binaries))
                    .subscribeOn(Schedulers.boundedElastic())
                    .doOnError(e -> getLogger().error("==> Error processing GridFS document: " + doc.getName(), e))
                    .doFinally(signal ->mongoClient.close());
        }
    }

    // For non-GridFS
    private Document insertDocument(Document doc, Map<String, MetadataType> metadata) {
        getLogger().debug("==> In insertDocument() ");

        //  Create new insert document
        insertDoc = new org.bson.Document();

        // Iterate through the metadata map and insert keys/values into document
        for (Map.Entry<String, MetadataType> entry : metadata.entrySet()) {
            getLogger().trace("==> key = " + entry.getKey());
            getLogger().trace("==> key value = " + extractMetadataStringValue(entry.getKey(), entry.getValue()));
            insertDoc.put(entry.getKey(), extractMetadataStringValue(entry.getKey(), entry.getValue()));
        }
        getLogger().debug("==> Metadata added to document");

        // Add Simflofy-specific metadata to document
        addSimMeta(insertDoc, doc);
        getLogger().debug("==> Simflofy metadata added to document");

        //  Write document
        InsertOneResult result = mongoCollection.insertOne(insertDoc);
        getLogger().debug("==> Status: " + result + ", Inserted document: " + doc.getName());

        return doc;
    }

    // For GridFS
    private Document writeContent(Document doc, Map<String, MetadataType> metadata, Flux<DataBuffer> binaries) {
        getLogger().debug("==> In writeContent() ");

        //  Create new insert document
        insertDoc = new org.bson.Document();

        // Iterate through the metadata map and insert keys/values into document
        for (Map.Entry<String, MetadataType> entry : metadata.entrySet()) {
            getLogger().trace("==> key = " + entry.getKey());
            getLogger().trace("==> key value = " + extractMetadataStringValue(entry.getKey(), entry.getValue()));
            insertDoc.put(entry.getKey(), extractMetadataStringValue(entry.getKey(), entry.getValue()));
        }
        getLogger().debug("==> Metadata added to document");

        // Add Simflofy-specific metadata to document
        addSimMeta(insertDoc, doc);
        getLogger().debug("==> Simflofy metadata added to document");

        // Write document
        GridFSUploadOptions options = new GridFSUploadOptions();
        options.metadata(insertDoc);
        gridFSBucket.uploadFromStream(doc.getName(), convert(binaries), options);
        getLogger().debug("==> Inserted document: " + doc.getName());

        return doc;
    }

    // Add Simflofy-specific metadata to document
    private void addSimMeta(org.bson.Document meta, Document doc) {
        meta.put(SIMFLOFY_CREATED_BY, SIMFLOFY);
        meta.put(SIMFLOFY_CREATED, new Date());
        meta.put(SIMFLOFY_PATH_FIELD, doc.getParentPath());
        meta.put(SIMFLOFY_DOWNLOADABLE_FIELD, false);
        meta.put(SIMFLOFY_FILENAME_FIELD, doc.getName());
        meta.put(LAST_MODIFIED, new Date(doc.getModifiedDate().getSeconds() * 1000));
        meta.put(CREATED, new Date(doc.getCreatedDate().getSeconds() * 1000));
        meta.put(SIMFLOFY_TYPENAME, DOCUMENT);
        meta.put(SIMFLOFY_LAST_MODIFIED, new Date());
        meta.put(SIMFLOFY_LAST_MODIFIED_BY, SIMFLOFY);
        meta.put(SIMFLOFY_CONTENT_TYPE_FIELD, doc.getMimeType());
        meta.put(SIMFLOFY_LENGTH_FIELD, doc.getSize());
        meta.put(SIMFLOFY_SOURCE_REPOSITORY_ID_FIELD, doc.getId());
    }

    // Extract String value from metadata
    String extractMetadataStringValue(String key, MetadataType value) {
        if (value.hasArray()) {
            return value.getArray().toString();
        } else if (value.hasBinary()) {
            return Hex.encodeHexString(value.getBinary().toByteArray());
        } else if (value.hasBoolean()) {
            return String.valueOf(value.getBoolean());
        } else if (value.hasDouble()) {
            return String.valueOf(value.getDouble());
        } else if (value.hasDecimal()) {
            return String.valueOf(value.getDecimal());
        } else if (value.hasDateTime()) {
            return convertTimestampToUTCString(value.getDateTime());
        } else if (value.hasInteger()) {
            return String.valueOf(value.getInteger());
        } else if (value.hasLargeString()) {
            return value.getLargeString();
        } else if (value.hasLong()) {
            return String.valueOf(value.getLong());
        } else if (value.hasString()) {
            return value.getString();
        }
        getLogger().warn("Incompatible type. No value found for metadata key " + key);
        return null;
    }

    // Converts Timestamp to UTC String
    String convertTimestampToUTCString(Timestamp timestamp) {
        Instant instant = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
        return DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC).format(instant);
    }

    // Converts Flux<DataBuffer> to InputStream
    public InputStream convert(Flux<DataBuffer> dataBufferFlux) {
        return dataBufferFlux.map(DataBuffer::asInputStream)
                .reduce(SequenceInputStream::new)
                .block();
    }
}