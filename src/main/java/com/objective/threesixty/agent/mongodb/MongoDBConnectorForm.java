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

import com.objective.threesixty.*;
import com.objective.threesixty.remoteagent.sdk.agent.ConnectorForm;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.objective.threesixty.agent.mongodb.MongoDBConstants.*;
import static com.objective.threesixty.agent.mongodb.MongoDBConstants.QUERY;

@Component
public class MongoDBConnectorForm implements ConnectorForm {
    @Override
    public List<Field> getSourceRepositoryFields() {
        Field connectionString = Field.newBuilder()
                .setLabel("Connection String")
                .setId(URI)
                .setTextField(TextField.newBuilder()
                        .build())
                .build();

        Field db = Field.newBuilder()
                .setLabel("Database")
                .setId(DB)
                .setTextField(TextField.newBuilder()
                        .build())
                .build();

        Field collection = Field.newBuilder()
                .setLabel("Collection")
                .setId(COLLECTION)
                .setTextField(TextField.newBuilder()
                        .build())
                .build();

        Field idField = Field.newBuilder()
                .setLabel("ID Field (defaults to '_id', if left blank)")
                .setId(ID_FIELD)
                .setTextField(TextField.newBuilder()
                        .build())
                .build();

        Field query = Field.newBuilder()
                .setLabel("Query")
                .setId(QUERY)
                .setTextAreaField(TextAreaField.newBuilder()
                        .setValue("{}")
                        .build())
                .build();

        Field gridFS = Field.newBuilder()
                .setLabel("Use GridFS")
                .setId(USE_GRIDFS)
                .setCheckboxField(CheckboxField.newBuilder()
                        .setValue(false)
                        .build())
                .build();

        return List.of(connectionString, db, collection, idField, query, gridFS);
    }

    @Override
    public List<Field> getOutputRepositoryFields() {
        Field connectionString = Field.newBuilder()
                .setLabel("Connection String")
                .setId(URI)
                .setTextField(TextField.newBuilder()
                        .build())
                .build();

        Field db = Field.newBuilder()
                .setLabel("Database")
                .setId(DB)
                .setTextField(TextField.newBuilder()
                        .build())
                .build();

        Field collection = Field.newBuilder()
                .setLabel("Collection")
                .setId(COLLECTION)
                .setTextField(TextField.newBuilder()
                        .build())
                .build();

        Field gridFS = Field.newBuilder()
                .setLabel("Use GridFS")
                .setId(USE_GRIDFS)
                .setCheckboxField(CheckboxField.newBuilder()
                        .build())
                .build();

        return List.of(connectionString, db, collection, gridFS);
    }

    @Override
    public List<Field> getContentServiceFields() {
        // TODO Finish This
        Field toggleCheckbox = Field.newBuilder()
                .setLabel("Show/Hide Text field")
                .setDescription("Toggles Text field")
                .setId("toggleCheckbox")
                .setCheckboxField(CheckboxField.newBuilder().setValue(false).build())
                .build();
        Field textField = Field.newBuilder()
                .setLabel("Text field")
                .setId("textField")
                .setDependsOn(toggleCheckbox.getId())
                .setTextField(TextField.newBuilder().build())
                .build();
        Field numberField = Field.newBuilder()
                .setLabel("Number field")
                .setId("numberField")
                .setNumberField(NumberField.newBuilder().build())
                .setDependsOn("noMatchingId")
                .build();
        return List.of(toggleCheckbox, textField, numberField);
    }
}