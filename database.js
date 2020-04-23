const assert = require("assert");
const database = "invoicesdb";

var MongoClient = require('mongodb').MongoClient;
var dbInstance = null;

/* initialize DB connection */
function initDb(callback) {
   if (dbInstance) {
      console.warn("Trying to init DB again!");
      return callback();
   }

   /* connect to the db */
   MongoClient.connect("mongodb://127.0.0.1:27017",
      { useNewUrlParser: true, useUnifiedTopology: true },
      (err, client) => {
         if (err) {
            throw err;
         }
         db = client.db(database);
         console.log("Connected to database: " + database);
         dbInstance = db;
         return callback();
      }
   )
}

/* get DB instance */
function getDb() {
   assert.ok(dbInstance, "Db has not been initialized. Please called init first.");
   return dbInstance;
}

module.exports = {
   getDb,
   initDb
};