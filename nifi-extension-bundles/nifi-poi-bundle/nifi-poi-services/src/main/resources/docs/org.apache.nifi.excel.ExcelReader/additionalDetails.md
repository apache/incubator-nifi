<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# ExcelReader

The ExcelReader allows for interpreting input data as delimited Records. Each row in an Excel spreadsheet is a record
and each cell is considered a field. The reader allows for choosing which row to start from and which sheets in a
spreadsheet to ingest. When using the "Use Starting Row" strategy, the field names will be assumed to be the column
names from the configured starting row. If there are any column(s) from the starting row which are blank, they are
automatically assigned a field name using the cell number prefixed with "column\_". When using the "Infer Schema"
strategy, the field names will be assumed to be the cell numbers of each column prefixed with "column\_". Otherwise, the
names of fields can be supplied when specifying the schema by using the Schema Text or looking up the schema in a Schema
Registry.

## Schemas and Type Coercion

When a record is parsed from incoming data, it is separated into fields. Each of these fields is then looked up against
the configured schema (by field name) in order to determine what the type of the data should be. If the field is not
present in the schema, that field is omitted from the Record. If the field is found in the schema, the data type of the
received data is compared against the data type specified in the schema. If the types match, the value of that field is
used as-is. If the schema indicates that the field should be of a different type, then the Controller Service will
attempt to coerce the data into the type specified by the schema. If the field cannot be coerced into the specified
type, an Exception will be thrown.

The following rules apply when attempting to coerce a field value from one data type to another:

* Any data type can be coerced into a String type.
* Any numeric data type (Byte, Short, Int, Long, Float, Double) can be coerced into any other numeric data type.
* Any numeric value can be coerced into a Date, Time, or Timestamp type, by assuming that the Long value is the number
  of milliseconds since epoch (Midnight GMT, January 1, 1970).
* A String value can be coerced into a Date, Time, or Timestamp type, if its format matches the configured "Date
  Format," "Time Format," or "Timestamp Format."
* A String value can be coerced into a numeric value if the value is of the appropriate type. For example, the String
  value `8` can be coerced into any numeric type. However, the String value `8.2` can be coerced into a Double or Float
  type but not an Integer.
* A String value of "true" or "false" (regardless of case) can be coerced into a Boolean value.
* A String value that is not empty can be coerced into a Char type. If the String contains more than 1 character, the
  first character is used and the rest of the characters are ignored.
* Any "date/time" type (Date, Time, Timestamp) can be coerced into any other "date/time" type.
* Any "date/time" type can be coerced into a Long type, representing the number of milliseconds since epoch (Midnight
  GMT, January 1, 1970).
* Any "date/time" type can be coerced into a String. The format of the String is whatever DateFormat is configured for
  the corresponding property (Date Format, Time Format, Timestamp Format property).

If none of the above rules apply when attempting to coerce a value from one data type to another, the coercion will fail
and an Exception will be thrown.

## Use Starting Row and Schema Inference

While NiFi's Record API does require that each Record have a schema, it is often convenient to infer the schema based on
the values in the data, rather than having to manually create a schema. This is accomplished by selecting either value
of "Use Starting Row" or "Infer Schema" for the "Schema Access Strategy" property. When using the "Use Starting Row"
strategy, the Reader will determine the schema by parsing the first ten rows after the configured starting row of the
data in the FlowFile all the while keeping track of all fields that it has encountered and the type of each field. A
schema is then formed that encompasses all encountered fields. A schema can even be inferred if there are blank lines
within those ten rows, but if they are all blank, then this strategy will fail to create a schema. When using the "Infer
Schema" strategy, the Reader will determine the schema by first parsing all data in the FlowFile, keeping track of all
fields that it has encountered and the type of each field. Once all data has been parsed, a schema is formed that
encompasses all fields that have been encountered.

A common concern when inferring schemas is how to handle the condition of two values that have different types. For
example, consider a FlowFile with the following two records:

```
name, age
John, 8
Jane, Ten
```

It is clear that the "name" field will be inferred as a STRING type. However, how should we handle the "age" field?
Should the field be an CHOICE between INT and STRING? Should we prefer LONG over INT? Should we just use a STRING?
Should the field be considered nullable?

To help understand how this Record Reader infers schemas, we have the following list of rules that are followed in the
inference logic:

* All fields are inferred to be nullable.
* When two values are encountered for the same field in two different records (or two values are encountered for an
  ARRAY type), the inference engine prefers to use a "wider" data type over using a CHOICE data type. A data type "A" is
  said to be wider than data type "B" if and only if data type "A" encompasses all values of "B" in addition to other
  values. For example, the LONG type is wider than the INT type but not wider than the BOOLEAN type (and BOOLEAN is also
  not wider than LONG). INT is wider than SHORT. The STRING type is considered wider than all other types except MAP,
  RECORD, ARRAY, and CHOICE.
* Before inferring the type of value, leading and trailing whitespace are removed. Additionally, if the value is
  surrounded by double-quotes ("), the double-quotes are removed. Therefore, the value `16` is interpreted the same as
  `"16"`. Both will be interpreted as an INT. However, the value `" 16"` will be inferred as a STRING type because the
  white space is enclosed within double-quotes, which means that the white space is considered part of the value.
* If the "Time Format," "Timestamp Format," or "Date Format" properties are configured, any value that would otherwise
  be considered a STRING type is first checked against the configured formats to see if it matches any of them. If the
  value matches the Timestamp Format, the value is considered a Timestamp field. If it matches the Date Format, it is
  considered a Date field. If it matches the Time Format, it is considered a Time field. In the unlikely event that the
  value matches more than one of the configured formats, they will be matched in the order: Timestamp, Date, Time. I.e.,
  if a value matched both the Timestamp Format and the Date Format, the type that is inferred will be Timestamp. Because
  parsing dates and times can be expensive, it is advisable not to configure these formats if dates, times, and
  timestamps are not expected, or if processing the data as a STRING is acceptable. For use cases when this is
  important, though, the inference engine is intelligent enough to optimize the parsing by first checking several very
  cheap conditions. For example, the string's length is examined to see if it is too long or too short to match the
  pattern. This results in far more efficient processing than would result if attempting to parse each string value as a
  timestamp.
* The MAP type is never inferred.
* The ARRAY type is never inferred.
* The RECORD type is never inferred.
* If a field exists but all values are null, then the field is inferred to be of type STRING.

## Caching of Inferred Schemas

This Record Reader requires that if a schema is to be inferred, that all records be read in order to ensure that the
schema that gets inferred is applicable for all records in the FlowFile. However, this can become expensive, especially
if the data undergoes many different transformations. To alleviate the cost of inferring schemas, the Record Reader can
be configured with a "Schema Inference Cache" by populating the property with that name. This is a Controller Service
that can be shared by Record Readers and Record Writers.

Whenever a Record Writer is used to write data, if it is configured with a "Schema Cache," it will also add the schema
to the Schema Cache. This will result in an identifier for that schema being added as an attribute to the FlowFile.

Whenever a Record Reader is used to read data, if it is configured with a "Schema Inference Cache", it will first look
for a "schema.cache.identifier" attribute on the FlowFile. If the attribute exists, it will use the value of that
attribute to lookup the schema in the schema cache. If it is able to find a schema in the cache with that identifier,
then it will use that schema instead of reading, parsing, and analyzing the data to infer the schema. If the attribute
is not available on the FlowFile, or if the attribute is available but the cache does not have a schema with that
identifier, then the Record Reader will proceed to infer the schema as described above.

The end result is that users are able to chain together many different Processors to operate on Record-oriented data.
Typically, only the first such Processor in the chain will incur the "penalty" of inferring the schema. For all other
Processors in the chain, the Record Reader is able to simply lookup the schema in the Schema Cache by identifier. This
allows the Record Reader to infer a schema accurately, since it is inferred based on all data in the FlowFile, and still
allows this to happen efficiently since the schema will typically only be inferred once, regardless of how many
Processors handle the data.

## Examples

### Example 1

As an example, consider a FlowFile whose contents are an Excel spreadsheet whose only sheet consists of the following:

```
id, name, balance, join_date, notes
1, John, 48.23, 04/03/2007, "Our very   first customer!"
2, Jane, 1245.89, 08/22/2009,	  
3, Frank Franklin, "48481.29", 04/04/2016,
```

Additionally, let's consider that this Controller Service is configured to skip the first line and is configured with
the Schema Registry pointing to an AvroSchemaRegistry which contains the following schema:

```json
{
  "namespace": "nifi",
  "name": "balances",
  "type": "record",
  "fields": [
    {
      "name": "id",
      "type": "int"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "balance",
      "type": "double"
    },
    {
      "name": "join_date",
      "type": {
        "type": "int",
        "logicalType": "date"
      }
    },
    {
      "name": "notes",
      "type": "string"
    }
  ]
}
```

In the example above, we see that the 'join\_date' column is a Date type. In order for the Excel Reader to be able to
properly parse a value as a date, we need to provide the reader with the date format to use. In this example, we would
configure the Date Format property to be `MM/dd/yyyy` to indicate that it is a two-digit month, followed by a two-digit
day, followed by a four-digit year - each separated by a slash. In this case, the result will be that this FlowFile
consists of 3 different records. The first record will contain the following values:

| Field Name | Field Value                   |
|------------|-------------------------------|
| id         | 1                             |
| name       | John                          |
| balance    | 48.23                         |
| join\_date | 04/03/2007                    |
| notes      | Our very  <br>first customer! |

The second record will contain the following values:

| Field Name | Field Value |
|------------|-------------|
| id         | 2           |
| name       | Jane        |
| balance    | 1245.89     |
| join\_date | 08/22/2009  |
| notes      |             |

The third record will contain the following values:

| Field Name | Field Value    |
|------------|----------------|
| id         | 3              |
| name       | Frank Franklin |
| balance    | 48481.29       |
| join\_date | 04/04/2016     |
| notes      |                |