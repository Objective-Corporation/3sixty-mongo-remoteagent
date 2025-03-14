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

import com.mongodb.client.*;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.objective.threesixty.Document;
import com.objective.threesixty.MetadataType;
import com.objective.threesixty.remoteagent.sdk.BinaryDetails;
import com.objective.threesixty.remoteagent.sdk.agent.RepositoryReader;
import com.objective.threesixty.remoteagent.sdk.utils.CustomParameters;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Stream;
import static com.mongodb.client.model.Filters.eq;
import static com.objective.threesixty.agent.mongodb.MongoDBConstants.*;

@Component
@Scope("singleton")
public class MongoDBReader implements RepositoryReader {

    private MongoDatabase database;
    private boolean gridFS;
    private String idField;
    private String query;
    private String collectionStr;
    private boolean useObjectId;
    private long startTime;
    private long endTime;

    @Override
    public void init(CustomParameters parameters) {
        getLogger().debug("==> In init() ");

        // Retrieve all stored parameters for this Remote Agent

        // Check if GridFS is used
        gridFS = parameters.get(USE_GRIDFS).getBoolean();

        // Get MongoDB Connection String
        String uri = parameters.get(URI).getString();

        // Get MongoDB Database
        String db = parameters.get(DB).getString();

        // Get Collection
        this.collectionStr = parameters.get(COLLECTION).getString();

        // Get ID Field for Collection
        String idField = parameters.get(ID_FIELD).getString();

        // Get Query
        this.query = parameters.get(QUERY).getString();

        // Creates a MongoDB Client using the stored Connection String parameter value
        MongoClient mongoClient = MongoClients.create(uri);
        getLogger().debug("==> MongoClient created using URI = " + uri);

        // Sets MongoDB Database to the stored Database parameter value
        this.database = mongoClient.getDatabase(db);

        getLogger().debug("==> Database = " + db + ", Collection = " + collectionStr);

        // Checks if an ID field has been provided
        // If not, use "_id" ObjectId and set the useObjectId boolean value
        this.idField = StringUtils.isEmpty(idField) ? _ID : idField;
        this.useObjectId = this.idField.equals(_ID);
        getLogger().debug("==> ID Field = " + idField);

        // Get the start time and end time filters
        this.startTime = parameters.getStartTimeOfDateFilter();
        this.endTime = parameters.getEndTimeOfDateFilter();
        getLogger().debug("==> Start Time = " + startTime);
        getLogger().debug("==> End Time = " + endTime);
    }

    @Override
    public Stream<Document> getDocuments(CustomParameters parameters) {
        getLogger().debug("==> In getDocuments() ");

        // Create Document Set to hold documents to be returned
        Set<Document> docs = new HashSet<>();

        // Create MongoDB Query using stored query predicate parameter value
        org.bson.Document queryDoc = org.bson.Document.parse(query);
        getLogger().debug("==> Create Query Document using query = " + query);

        // Create MongoDB Iterable to hold results from query
        MongoIterable<?> results;

        // Check if GridFS is used
        if (gridFS) {
            // Create GridFS Bucket using database and collection
            GridFSBucket gridFSBucket = GridFSBuckets.create(database, collectionStr);
            getLogger().debug("==> Create GridFSBucket from database using collection = " + collectionStr);

            //  Add filter to retrieve documents within the specified start and end times
            setQueryDateFields(startTime, endTime, queryDoc);

            // Execute query for GridFS
            results = gridFSBucket.find(queryDoc);
        } else {
            // Get collection from database
            MongoCollection<org.bson.Document> collection = database.getCollection(collectionStr);
            getLogger().debug("==> Get Collection from database using collection = " + collectionStr);

            // Execute query for non-GridFS
            results = collection.find(queryDoc);
        }

        // Iterate for each document returned from query
        for (Object obj : results) {
            String docId = null;

            // Check if GridFS is used
            if (gridFS) {
                // Cast object returned to GridFSFile
                GridFSFile f = (GridFSFile) obj;

                // Set docId for GridFS
                if (!idField.equals("_id") && f.getMetadata() != null && f.getMetadata().containsKey(idField)) {
                    docId = (String.valueOf(f.getMetadata().get(idField)));
                } else if (idField.equals("_id") && f.getMetadata() != null) {
                    docId = (String.valueOf(f.getObjectId()));
                }

            } else {
                // Cast object returned to BSON document
                org.bson.Document objDoc = (org.bson.Document) obj;

                // Set docId for non-GridFS
                if (!idField.equals("_id") && objDoc.containsKey(idField)) {
                    docId = String.valueOf(objDoc.get(idField));
                } else {
                    docId = String.valueOf(objDoc.getObjectId("_id"));
                }
            }

            // Check if ID Field is found
            if (docId != null) {
                // Iterate and call getDocument() for each document, then add to Document Set
                try {
                    Document doc = getDocument(docId, parameters);
                    docs.add(doc);
                } catch (Exception e) {
                    getLogger().error("==> Exception in calling getDocument()", e);
                }
            } else {
                getLogger().error("==> Could not find ID Field '" + idField +"' specified in document");
                break;
            }
        }
        getLogger().debug("==> Returning " + docs.size() + " documents");

        // Return the Stream of documents retrieved
        return docs.stream();
    }

    @Override
    public Document getDocument(String docId, CustomParameters parameters) {
        getLogger().debug("==> In getDocument() for docId = " + docId);

        // Create document attributes for building document to be returned
        String docName ="";
        String contentType = "";
        long size = 0L;

        // Create Filter to return single document with docId
        Bson eq;
        if (idField.equals("_id")) {
            eq = eq(idField, new ObjectId(docId));
        } else {
            if (gridFS) {
                eq = eq(METADATA_DOT+idField, docId);
            } else {
                eq = eq(idField, docId);
            }
        }

        // Check if using GridFS
        if (gridFS) {
            // Execute Query to find single document using docId
            GridFSBucket gridFSBucket = GridFSBuckets.create(database, parameters.get(COLLECTION).getString());
            GridFSFile first = gridFSBucket.find(eq).first();

            // If document found, get document attributes
            if (first != null && first.getMetadata() != null) {
                org.bson.Document metadata = first.getMetadata();
                docName = first.getFilename();
                size = first.getLength();
                contentType = String.valueOf(metadata.get(SIMFLOFY_CONTENT_TYPE_FIELD));

                // Build document using document attributes
                Document doc = Document.newBuilder()
                        .setId(docId)
                        .setName(docName)
                        .setMimeType(contentType)
                        .setSize(size)
                        .build();

                getLogger().debug("==> Adding GridFS document with docId =  " + docId + ", docName = " + docName +
                        ", contentType = " + contentType + ", size = " + size);

                // Return built document
                return doc;

            } else {
                getLogger().error("==> Could not find GridFS doc: " + docId);
            }
        } else {
            // Execute Query to find single document using docId
            MongoCollection<org.bson.Document> collection = database.getCollection(parameters.get(COLLECTION).getString());
            org.bson.Document first = collection.find(eq).first();

            // If document found, get document attributes
            if (first != null) {

                // Iterate through the keys to pull document attributes
                for (String key : first.keySet()) {
                    getLogger().trace("==> key = " + key);
                    getLogger().trace("==> key value = " + first.get(key));

                    // Get document attributes
                    switch (key) {
                        case "simflofy_filename" -> docName = first.get(key).toString();
                        case "simflofy_content_type" -> contentType = first.get(key).toString();
                        case "simflofy_length" -> size = Long.parseLong(first.get(key).toString());
                    }
                }

                // Build document using document attributes
                Document doc = Document.newBuilder()
                        .setId(docId)
                        .setName(docName)
                        .setMimeType(contentType)
                        .setSize(size)
                        .build();

                getLogger().debug("==> Adding document with docId =  " + docId + ", docName = " + docName +
                        ", contentType = " + contentType + ", size = " + size);

                // Return built document
                return doc;

            } else {
                getLogger().error("==> Could not find non-GridFS document: " + docId);
            }
        }
        // Returns null document if unable to find document with docId
        return null;
    }

    @Override
    public Map<String, MetadataType>
    getDocumentMetadata(String docId, CustomParameters parameters) {
        getLogger().debug("==> In getDocumentMetadata() ");

        // Create filter to return metadata from single document
        Bson eq;
        if (idField.equals("_id")) {
            eq = eq(idField, new ObjectId(docId));
        } else {
            if (gridFS) {
                eq = eq(METADATA_DOT+idField, docId);
            } else {
                eq = eq(idField, docId);
            }
        }

        // Create MetadataTypeMap to hold metadata from document
        Map<String, MetadataType> metadataTypeMap = new HashMap<>();

        // Check if using GridFS
        if (gridFS) {
            // Execute Query to find single document using docId
            GridFSBucket gridFSBucket = GridFSBuckets.create(database, parameters.get(COLLECTION).getString());
            GridFSFile first = gridFSBucket.find(eq).first();

            // If document found, get document's metadata
            if (first != null && first.getMetadata() != null) {
                getLogger().debug("*** New Document with docID = " + docId + " ***");
                org.bson.Document metadata = first.getMetadata();

                // Iterate through the keys to pull and store document metadata
                for (String key : metadata.keySet()) {
                    getLogger().trace("==> key = " + key);
                    getLogger().trace("==> key value = " + metadata.get(key));
                    metadataTypeMap.put(key, MetadataType.newBuilder().setString(String.valueOf(metadata.get(key))).build());
                }
            } else {
                getLogger().error("==> getDocumentMetadata():  Could not find GridFS document: " + docId);
            }

        } else {
            // Execute Query to find single document using docId
            MongoCollection<org.bson.Document> collection = database.getCollection(parameters.get(COLLECTION).getString());
            org.bson.Document first = collection.find(eq).first();

            // If document found, get document metadata
            if (first != null) {
                getLogger().debug("*** New Document with docID = " + docId + " ***");

                // Iterate through the keys to pull and store document metadata
                for (String key : first.keySet()) {
                    getLogger().trace("==> key = " + key);
                    getLogger().trace("==> key value = " + first.get(key));
                    metadataTypeMap.put(key, MetadataType.newBuilder().setString(String.valueOf(first.get(key))).build());
                }
            } else {
                getLogger().error("==> getDocumentMetadata():  Could not find non-GridFS document: " + docId);
            }
        }
        // Returns the metadataTypeMap
        return metadataTypeMap;
    }

    @Override
    public BinaryDetails getDocumentBinary(String docId, CustomParameters parameters) {
        getLogger().debug("==> In getDocumentBinary() ");

        // Create BinaryDetails with null input stream and default MIME type
        BinaryDetails bd = new BinaryDetails(docId, InputStream.nullInputStream(), "application/octet-stream");

        // Check if using GridFS
        if (gridFS) {
            // Create filter to find document with docId
            Bson eq;
            if (idField.equals("_id")) {
                eq = eq(idField, new ObjectId(docId));
            } else {
                eq = eq(METADATA_DOT + idField, docId);
            }

            // Execute query to return single document
            GridFSBucket gridFSBucket = GridFSBuckets.create(database, parameters.get(COLLECTION).getString());
            GridFSFile first = gridFSBucket.find(eq).first();

            // If document found, get content type attribute, defaults to "application/octet-stream"
            if (first != null) {
                String mimetype = first.getMetadata() != null ? String.valueOf(first.getMetadata().get(SIMFLOFY_CONTENT_TYPE_FIELD)) : "application/octet-stream";

                // Set input stream and MIME type of document
                bd.setInputStream(gridFSBucket.openDownloadStream(first.getObjectId()));
                bd.setMimeType(mimetype);
            }
        }
        // Return BinaryDetails
        return bd;
    }

    @Override
    public void deleteDocument(String docId, CustomParameters parameters) {
        getLogger().debug("==> In deleteDocument() for docId = " + docId);

        // Create Filter to delete single document with docId
        Bson eq;
        if (idField.equals("_id")) {
            eq = eq(idField, new ObjectId(docId));
        } else {
            if (gridFS) {
                eq = eq(METADATA_DOT+idField, docId);
            } else {
                eq = eq(idField, docId);
            }
        }

        // Check if using GridFS
        if (gridFS) {
            // Create GridFSBucket
            GridFSBucket gridFSBucket = GridFSBuckets.create(database, parameters.get(COLLECTION).getString());

            // Set fileId to use for delete operation
            ObjectId fileId = null;
            if (useObjectId) {
                fileId = new ObjectId(docId);
            } else {
                GridFSFile first = gridFSBucket.find(eq).first();
                if (first != null && first.getMetadata() != null) {
                    fileId = new ObjectId(String.valueOf(first.getObjectId()));
                }
            }

            if (fileId!=null) {
                // Execute delete of GridFS document using Object ID
                gridFSBucket.delete(fileId);
                getLogger().debug("==> Document deleted: " + docId);
            } else {
                getLogger().error("==> Could not delete: " + docId);
            }

        } else {
            // Create MongoCollection
            MongoCollection<org.bson.Document> collection = database.getCollection(parameters.get(COLLECTION).getString());

            // Execute delete of non-GridFS document using Filter
            collection.deleteOne(eq);
            getLogger().debug("==> Document deleted: " + docId);
        }

    }

    // Add start time and end time filters to query
    private void setQueryDateFields(long startTime, long endTime, org.bson.Document queryObject) {
        org.bson.Document tdoc = new org.bson.Document();
        if (startTime > 0L) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(startTime);
            tdoc.put("$gte", cal.getTime());
        }
        if (endTime > 0L) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(endTime);
            tdoc.put("$lte", cal.getTime());
        }
        if (!tdoc.isEmpty()) {
            String queryField = METADATA_DOT + LAST_MODIFIED;
            queryObject.put(queryField, tdoc);
        }
    }

}