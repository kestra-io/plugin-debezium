use kestra;

// Drop existing collections if they exist
db.mongo_types.drop();
db.second_employee_territory.drop();

// Create the mongo_types collection and insert documents
db.mongo_types.insertMany([
    {
        _id: 1,
        available: true,
        a: "four",
        b: "This is a varchar",
        c: "This is a text column data",
        d: null,
        play_time: NumberLong("-9223372036854775808"),
        library_record: NumberLong("1844674407370955161"),
        bitn_test: BinData(0, "AAU="),
        floatn_test: 9223372036854776000.0,
        double_test: 9223372036854776000.0,
        doublen_test: NumberDecimal("2147483645.1234"),
        numeric_test: NumberDecimal("5.36"),
        salary_decimal: NumberDecimal("999.99"),
        date_type: ISODate("2030-12-25T00:00:00Z"),
        datetime_type: ISODate("2050-12-31T22:59:57.150150Z"),
        time_type: "04:05:30",
        timestamp_type: ISODate("2004-10-19T10:23:54.999999Z"),
        year_type: 2025,
        json_type: { color: "red", value: "#f00" },
        blob_type: BinData(0, "3q2+7w==")
    },
    {
        _id: 2,
        available: true,
        a: "four",
        b: "This is a varchar",
        c: "This is a text column data",
        d: null,
        play_time: NumberLong("-9223372036854775808"),
        library_record: NumberLong("1844674407370955161"),
        bitn_test: BinData(0, "AAU="),
        floatn_test: 9223372036854776000.0,
        double_test: 9223372036854776000.0,
        doublen_test: NumberDecimal("2147483645.1234"),
        numeric_test: NumberDecimal("5.36"),
        salary_decimal: NumberDecimal("999.99"),
        date_type: ISODate("2030-12-25T00:00:00Z"),
        datetime_type: ISODate("2050-12-31T22:59:57.150150Z"),
        time_type: "04:05:30",
        timestamp_type: ISODate("2004-10-19T10:23:54.999999Z"),
        year_type: 2025,
        json_type: { color: "red", value: "#f00" },
        blob_type: BinData(0, "3q2+7w==")
    },
    {
        _id: 3,
        available: true,
        a: "four",
        b: "This is a varchar",
        c: "This is a text column data",
        d: null,
        play_time: NumberLong("-9223372036854775808"),
        library_record: NumberLong("1844674407370955161"),
        bitn_test: BinData(0, "AAU="),
        floatn_test: 9223372036854776000.0,
        double_test: 9223372036854776000.0,
        doublen_test: NumberDecimal("2147483645.1234"),
        numeric_test: NumberDecimal("5.36"),
        salary_decimal: NumberDecimal("999.99"),
        date_type: ISODate("2030-12-25T00:00:00Z"),
        datetime_type: ISODate("2050-12-31T22:59:57.150150Z"),
        time_type: "04:05:30",
        timestamp_type: ISODate("2004-10-19T10:23:54.999999Z"),
        year_type: 2025,
        json_type: { color: "red", value: "#f00" },
        blob_type: BinData(0, "3q2+7w==")
    }
]);

db.runCommand( {
   collMod: "mongo_types",
   changeStreamPreAndPostImages: { enabled: true }
} )

// Update a document
db.mongo_types.updateOne(
  { _id: 2 },
  { $set: { numeric_test: NumberDecimal("6.36") } }
);

db.mongo_types.deleteOne({ _id: 1 });

// Delete a document
db.mongo_types.remove({ numeric_test: NumberDecimal("5.36") });

use second;

// Create the second_employee_territory collection and insert documents
db.employeeTerritory.insertMany([
  { _id: 1, employeeId: 1, territoryId: '06897' },
  { _id: 2, employeeId: 1, territoryId: '19713' },
  { _id: 3, employeeId: 2, territoryId: '01581' },
  { _id: 4, employeeId: 2, territoryId: '01730' },
  { _id: 5, employeeId: 2, territoryId: '01833' },
  { _id: 6, employeeId: 2, territoryId: '02116' },
  { _id: 7, employeeId: 2, territoryId: '02139' }
]);

db.runCommand( {
   collMod: "employeeTerritory",
   changeStreamPreAndPostImages: { enabled: true }
} )

// Delete all documents in the second_employee_territory collection
db.employeeTerritory.deleteMany({});