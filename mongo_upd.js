var MongoClient = require('mongodb').MongoClient
var fs = require('fs');
if (!process.argv[2] || !process.argv[3] || !process.argv[4]) {
   console.log("Arguments are not proper - verify");
   console.log("Usage: import2mongo <sourcefilepath> <dbname> <collectioname>");
} else {
   var INPUT_FILE = process.argv[2];
   var DB_NAME = process.argv[3];
   var COLLECTION_NAME = process.argv[4];
   var DB_SERVER = "mongodb://127.0.0.1:27017";

   /* Connect to db */
   MongoClient.connect(DB_SERVER, { useNewUrlParser: true, useUnifiedTopology: true }, (err, client) => {
      if (err) {
         throw err;
      } else {
         console.log("We are connected to Mongodb");
         var db = client.db(DB_NAME);
         fs.readFile(INPUT_FILE, (err, data) => {
            if (err) {
               throw err;
            } else {
               db.collection(COLLECTION_NAME).insertMany(JSON.parse(data), (err) => {
                  if (err) {
                     throw err;
                  } else {
                     console.log("Data inserted");
                  }
                  client.close(); /* close connection to db when you are done with it */
               }); /* end of insertMany */
            }
         }); /* end of readFile */
      }
   });
}