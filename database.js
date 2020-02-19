const assert = require("assert");
const dateTime = require("date-and-time");
const dateFormat = require("dateformat");
const printf = require('printf');
var MongoClient = require('mongodb').MongoClient;

const database = "invoicesdb";
const empProjColl = "emp_proj";
const esaProjColl = "esa_proj";
const locLeaveColl = "loc_holiday";
const lastMonth = 11;
const firstMonth = 0;
const monthFirstDate = 1;
const monthLastDate = 0;


var dbInstance = null;

/* initialize DB connection */
function initDb(callback) {
   if (dbInstance) {
      console.warn("Trying to init DB again!");
      return callback();
   }

   /* connect to the db */
   MongoClient.connect("mongodb://localhost:27017",
      { useNewUrlParser: true, useUnifiedTopology: true },
      function (err, client) {
         if (err) {
            throw err;
         }
         db = client.db(database);
         console.log("Connected to database:" + database);
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


/* calculate number of weekdays(mon - fri) between dates */
function workDaysBetween(startDate, endDate) {
   return new Promise((resolve, reject) => {
      if (!startDate || isNaN(startDate) || !endDate || isNaN(endDate)) {
         reject(-1);
      } else {
         /* clone date to avoid messing up original data */
         var fromDate = new Date(startDate.getTime());
         var toDate = new Date(endDate.getTime());
         var daysBetween = 1;

         /* reset time */
         fromDate.setHours(0, 0, 0, 0);
         toDate.setHours(0, 0, 0, 0);

         while (fromDate < toDate) {
            fromDate.setDate(fromDate.getDate() + 1);
            var dayOfWeek = fromDate.getDay();
            /* check if the date is neither a Sunday(0) nor a Saturday(6) */
            if (dayOfWeek > 0 && dayOfWeek < 6) {
               daysBetween++;
            }
         }
         resolve(daysBetween);
      }
   });
};


function monthlyRevenueCompute(empJsonObj, revenueYear, monthIndex) {
   var sowStart = new Date(dateTime.parse(empJsonObj[0].sowStartDate, "DDMMYYYY", true));
   var sowEnd = new Date(dateTime.parse(empJsonObj[0].sowEndDate, "DDMMYYYY", true));
   var foreSeen = new Date(dateTime.parse(empJsonObj[0].foreseenEndDate, "DDMMYYYY", true));
   var billRatePerHr = parseInt(empJsonObj[0].billRatePerHr, 10);
   var billHourPerDay = parseInt(empJsonObj[0].wrkHrPerDay, 10);

   var intRevYear = parseInt(revenueYear, 10);

   var monthIdxStartDate = new Date(revenueYear, monthIndex, monthFirstDate);
   var monthIdxEndDate = new Date(revenueYear, monthIndex + 1, monthLastDate);

   var revStart = -1;
   var revEnd = -1;
   var revMonthStartDate = 0;
   var revMonthEndDate = 0;
   var revenueDays = 0;
   var revenueAmount = 0;
   var personalDays = 0
   var bufferDays = 0;
   var locationHolidays = 0;

   return new Promise((resolve, _reject) => {
      if (sowStart != 0 && !isNaN(sowStart)) {
         if (sowStart.getFullYear() === intRevYear) { /* sow start year is same as required year */
            revStart = sowStart;
         } else if (sowStart.getFullYear() < intRevYear) { /* sow start year is not the same as required year */
            revStart = new Date(intRevYear, firstMonth, 1);
         }
      }

      if (sowEnd != 0 && !isNaN(sowEnd)) {
         if (sowEnd.getFullYear() === intRevYear) { /* sow end year is same as required year */
            revEnd = sowEnd;
         } else if (sowEnd.getFullYear() > intRevYear) { /* sow start end is not the same as required year */
            revEnd = new Date(intRevYear, lastMonth, 0);
         }
      }

      var revStartMonth = new Date(revStart).getMonth();
      var revEndMonth = new Date(revEnd).getMonth();

      if (foreSeen != 0 && !isNaN(foreSeen)) {
         foreSeen = -1;
      }

      if (monthIndex === revStartMonth) {
         revMonthStartDate = new Date(intRevYear, monthIndex, revStart.getDate());
      } else {
         revMonthStartDate = new Date(intRevYear, monthIndex, monthFirstDate);
      }
      if (monthIndex === revEndMonth) {
         revMonthEndDate = new Date(intRevYear, monthIndex, revEnd.getDate());
      } else {
         revMonthEndDate = new Date(intRevYear, monthIndex + 1, monthLastDate);
      }

      return new Promise((resolve, _reject) => {
         workDaysBetween(revMonthStartDate, revMonthEndDate).then((daysBetween) => {
            resolve(daysBetween);
         });
      }).then((weekDays) => {
         if (monthIdxStartDate >= sowStart && monthIdxEndDate <= sowEnd) { /* check each month of selected year fall within SOW range */
            return new Promise((resolve, _reject) => {
               empJsonObj[0].leaves.forEach((leave) => {
                  var leaveStart = new Date(dateTime.parse(leave.startDate, "DDMMYYYY", true));
                  var leaveEnd = new Date(dateTime.parse(leave.endDate, "DDMMYYYY", true));
                  if (monthIndex === leaveStart.getMonth() && monthIndex !== leaveEnd.getMonth()) {
                     leaveEnd = revMonthEndDate;
                  } else if (monthIndex === leaveEnd.getMonth()) {
                     leaveStart = revMonthStartDate;
                  }

                  return new Promise(() => {
                     workDaysBetween(leaveStart, leaveEnd).then((daysBetween) => {
                        resolve(daysBetween);
                     });
                  }).then((daysBetween) => {
                     personalDays += parseInt(daysBetween, 10);
                  });
               });
               return (personalDays);
            }).then(() => { /* get buffer days */
               empJsonObj[0].buffers.forEach((buffer) => {
                  var bufferMonth = new Date(dateTime.parse(buffer.month, "MMYYYY", true)).getMonth();
                  if (monthIndex === bufferMonth) {
                     bufferDays += parseInt(buffer.days, 10);
                  }
               });
               return (bufferDays);
            }).then(() => { /* get public holidays */
               return new Promise((resolve, _reject) => {
                  empJsonObj[0].publicHolidays.forEach((publicHoliday) => {
                     var pubHolStart = new Date(dateTime.parse(publicHoliday.startDate, "DDMMYYYY", true));
                     var pubHolEnd = new Date(dateTime.parse(publicHoliday.endDate, "DDMMYYYY", true));
                     if (monthIndex === pubHolStart.getMonth() && monthIndex !== pubHolEnd.getMonth()) {
                        pubHolEnd = revMonthEndDate;
                     } else if (monthIndex === pubHolEnd.getMonth()) {
                        pubHolStart = revMonthStartDate;
                     }

                     return new Promise(() => {
                        workDaysBetween(pubHolStart, pubHolEnd).then((daysBetween) => {
                           resolve(daysBetween);
                        });
                     }).then((daysBetween) => {
                        locationHolidays += parseInt(daysBetween, 10);
                     });
                  });
                  return (locationHolidays);
               });
            }).then((locationHolidays) => {
               if (weekDays > 0) {
                  revenueDays = weekDays - (personalDays + bufferDays + locationHolidays);
                  if (revenueDays > 0) {
                     var revenueAmount = ((revenueDays * billHourPerDay) * billRatePerHr) / 100;
                  }
               }
               var revenueMonth = printf("%02s%04s", monthIndex + 1, intRevYear);
               var empRevenueObj = {'month': revenueMonth, 'startDate': dateFormat(revMonthStartDate, "dd-mmm-yyyy"), 'endDate': dateFormat(revMonthEndDate, "dd-mmm-yyyy"), 'revenue': revenueAmount};
               resolve(empRevenueObj);
            });
         } else {
            var revenueMonth = printf("%02s%04s", monthIndex + 1, intRevYear);
            var empRevenueObj = {'month': revenueMonth, 'startDate': dateFormat(revMonthStartDate, "dd-mmm-yyyy"), 'endDate': dateFormat(revMonthEndDate, "dd-mmm-yyyy"), 'revenue': revenueAmount};
            resolve(empRevenueObj);
         }
      });
   });
}


/* calculate revenue */
function calcEmpRevenue(empJsonObj, revenueYear) {
   let promises = [];
   return new Promise((resolve, _reject) => {
      for (var monthIndex = firstMonth; monthIndex <= lastMonth; monthIndex++) {
         promises.push(monthlyRevenueCompute(empJsonObj, revenueYear, monthIndex));
      }

      Promise.all(promises).then((result) => {
         resolve(result);
      });
   });
}


/* get list of projects */
function listAllProjects() {
   return new Promise((resolve, reject) => {
      db = getDb();
      var myCol = db.collection(esaProjColl);
      myCol.aggregate([
         {
            $project: {
               "_id": 1,
               "esaId": 2,
               "esaDesc": 3,
               "currency": 4,
               "billingMode": 5,
               "empEsaLink": 6
            }
         }
      ]).toArray(function (err, projectList) {
         if (err) {
            reject(err);
         } else {
            resolve(projectList);
         }
      });
   });
}


/* get employee list for selectd project */
function listEmployeeInProj(projId) {
   return new Promise((resolve, reject) => {
      db = getDb();
      var myCol = db.collection(empProjColl);
      myCol.aggregate([
         {
            $lookup: {
               from: "esa_proj",
               localField: "empEsaLink",
               foreignField: "empEsaLink",
               as: "empEsaProj"
            }
         },
         {
            $unwind: "$empEsaProj"
         },
         {
            $lookup: {
               from: "wrk_loc",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $match: {
               "esaId": projId
            }
         },
         {
            $project: {
               "_id": 1,
               "esaId": 2,
               "esaDesc": 3,
               "projName": 4,
               "ctsEmpId": 5,
               "empFname": 6,
               "empMname": 7,
               "empLname": 8,
               "lowesUid": 9,
               "deptName": 10,
               "sowStartDate": 11,
               "sowEndDate": 12,
               "foreseenEndDate": 13,
               "wrkCity": 14,
               "wrkHrPerDay": 15,
               "billRatePerHr": 16,
               "empEsaLink": 17,
               "projectionActive": 18
            }
         }
      ]).toArray(function (err, allProj) {
         if (err) {
            reject(err);
         } else {
            resolve(allProj);
         }
      });
   });
}


/* get projection data for specific employee in selected project */
function getEmployeeProjection(empEsaLink, ctsEmpId, revenueYear) {
   return new Promise((resolve, reject) => {
      db = getDb();
      db.collection(empProjColl).aggregate([
         {
            $lookup: {
               from: "esa_proj",
               localField: "empEsaLink",
               foreignField: "empEsaLink",
               as: "empEsaProj"
            }
         },
         {
            $unwind: "$empEsaProj"
         },
         {
            $lookup: {
               from: "wrk_loc",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $lookup: {
               from: "emp_leave",
               localField: "ctsEmpId",
               foreignField: "ctsEmpId",
               as: "empEsaLeave"
            }
         },
         {
            $unwind: "$empEsaLeave"
         },
         {
            $lookup: {
               from: "emp_buffer",
               localField: "ctsEmpId",
               foreignField: "ctsEmpId",
               as: "empEsaBuffer"
            }
         },
         {
            $unwind: "$empEsaBuffer"
         },
         {
            $lookup: {
               from: "loc_holiday",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empLocHoliday"
            }
         },
         {
            $unwind: "$empLocHoliday"
         },
         {
            $match: {
               "empEsaLink": empEsaLink,
               "ctsEmpId": ctsEmpId
            }
         },
         {
            $group: {
               "_id": "$_id",
               "esaId": { "$first": "$empEsaProj.esaId" },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "projName": { "$first": "$projName" },
               "ctsEmpId": { "$first": "$ctsEmpId" },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "wrkCity": { "$first": "$empEsaLoc.cityName" },
               "wrkHrPerDay": { "$first": "$wrkHrPerDay" },
               "billRatePerHr": { "$first": "$billRatePerHr" },
               "empEsaLink": { "$first": "$empEsaLink" },
               "projectionActive": { "$first": "$projectionActive" },
               "leaves": {
                  "$addToSet": {
                     "_id": "$empEsaLeave._id",
                     "startDate": "$empEsaLeave.startDate",
                     "endDate": "$empEsaLeave.endDate",
                     "days": "$empEsaLeave.days",
                     "reason": "$empEsaLeave.reason"
                  }
               },
               "buffers": {
                  "$addToSet": {
                     "_id": "$empEsaBuffer._id",
                     "month": "$empEsaBuffer.month",
                     "days": "$empEsaBuffer.days",
                     "reason": "$empEsaBuffer.reason"
                  }
               },
               "publicHolidays": {
                  "$addToSet": {
                     "_id": "$empLocHoliday._id",
                     "startDate": "$empLocHoliday.startDate",
                     "endDate": "$empLocHoliday.endDate",
                     "days": "$empLocHoliday.days",
                     "description": "$empLocHoliday.description"
                  }
               }
            }
         }
      ]).toArray(function (err, empDtl) {
         return new Promise((resolve, _reject) => {
            calcEmpRevenue(empDtl, revenueYear).then((revenueDetail) => {
               resolve(revenueDetail);
            });
         }).then((revenueDetail) => {
            empDtl.push({"revenue": revenueDetail});
            if (err) {
               reject(err);
            } else {
               resolve(empDtl);
            }
         });
      });
   });
}



//get leave days of all Employees
function getAllEmployeeLeaves() {
   return new Promise((resolve, reject) => {
      db = getDb();
      db.collection(empProjColl).aggregate([
         {
            $lookup: {
               from: "esa_proj",
               localField: "empEsaLink",
               foreignField: "empEsaLink",
               as: "empEsaProj"
            }
         },
         {
            $unwind: "$empEsaProj"
         },
         {
            $lookup: {
               from: "wrk_loc",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $lookup: {
               from: "emp_leave",
               localField: "ctsEmpId",
               foreignField: "ctsEmpId",
               as: "empEsaLeave"
            }
         },
         {
            $unwind: "$empEsaLeave"
         },
         {
            $group: {
               "_id": "$_id",
               "esaId": { "$first": "$empEsaProj.esaId" },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "ctsEmpId": { "$first": "$ctsEmpId" },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "wrkCity": { "$first": "$empEsaLoc.cityName" },
               "empEsaLink": { "$first": "$empEsaLink" },
               "projectionActive": { "$first": "$projectionActive" },
               "leave": {
                  "$push": {
                     "_id": "$empEsaLeave._id",
                     "month": "$empEsaLeave.month",
                     "startDate": "$empEsaLeave.startDate",
                     "endDate": "$empEsaLeave.endDate",
                     "days": "$empEsaLeave.days"
                  }
               }
            }
         }
      ]).toArray(function (err, oneProj) {
         if (err) {
            reject(err);
         } else {
            resolve(oneProj);
         }
      });
   });
}





//get projection data for all projects
function listAllEmployees() {
   return new Promise((resolve, reject) => {
      db = getDb();
      var myCol = db.collection(empProjColl);
      myCol.aggregate([
         {
            $lookup: {
               from: "esa_proj",
               localField: "empEsaLink",
               foreignField: "empEsaLink",
               as: "empEsaProj"
            }
         },
         {
            $unwind: "$empEsaProj"
         },
         {
            $lookup: {
               from: "wrk_loc",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $lookup: {
               from: "emp_leave",
               localField: "ctsEmpId",
               foreignField: "ctsEmpId",
               as: "empEsaLeave"
            }
         },
         {
            $unwind: "$empEsaLeave"
         },
         {
            $group: {
               "_id": "$_id",
               "esaId": { "$first": "$empEsaProj.esaId" },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "projName": { "$first": "$projName" },
               "ctsEmpId": { "$first": "$ctsEmpId" },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "wrkCity": { "$first": "$empEsaLoc.cityName" },
               "wrkHrPerDay": { "$first": "$wrkHrPerDay" },
               "billRatePerHr": { "$first": "$billRatePerHr" },
               "empEsaLink": { "$first": "$empEsaLink" },
               "projectionActive": { "$first": "$projectionActive" },
               "leave": {
                  "$push": {
                     "_id": "$empEsaLeave._id",
                     "month": "$empEsaLeave.month",
                     "startDate": "$empEsaLeave.startDate",
                     "endDate": "$empEsaLeave.endDate",
                     "days": "$empEsaLeave.days"
                  }
               }
            }
         }
      ]).toArray(function (err, oneProj) {
         if (err) {
            reject(err);
         } else {
            resolve(oneProj);
         }
      });
   });
}



//get projection data for all projects
function listActiveEmployeeInProj(projId) {
   return new Promise((resolve, reject) => {
      db = getDb();
      var myCol = db.collection(empProjColl);
      myCol.aggregate([
         {
            $lookup: {
               from: "esa_proj",
               localField: "empEsaLink",
               foreignField: "empEsaLink",
               as: "empEsaProj"
            }
         },
         {
            $unwind: "$empEsaProj"
         },
         {
            $lookup: {
               from: "wrk_loc",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $lookup: {
               from: "emp_leave",
               localField: "ctsEmpId",
               foreignField: "ctsEmpId",
               as: "empEsaLeave"
            }
         },
         {
            $unwind: "$empEsaLeave"
         },
         {
            $match: {
               "esaId": projId,
               "projectionActive": "1"
            }
         },
         {
            $group: {
               "_id": "$_id",
               "esaId": { "$first": "$empEsaProj.esaId" },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "projName": { "$first": "$projName" },
               "ctsEmpId": { "$first": "$ctsEmpId" },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "wrkCity": { "$first": "$empEsaLoc.cityName" },
               "wrkHrPerDay": { "$first": "$wrkHrPerDay" },
               "billRatePerHr": { "$first": "$billRatePerHr" },
               "empEsaLink": { "$first": "$empEsaLink" },
               "projectionActive": { "$first": "$projectionActive" },
               "leave": {
                  "$push": {
                     "_id": "$empEsaLeave._id",
                     "month": "$empEsaLeave.month",
                     "startDate": "$empEsaLeave.startDate",
                     "endDate": "$empEsaLeave.endDate",
                     "days": "$empEsaLeave.days"
                  }
               }
            }
         }
      ]).toArray(function (err, oneProj) {
         if (err) {
            reject(err);
         } else {
            resolve(oneProj);
         }
      });
   });
}



//get projection data for all projects
function listInactiveEmployeeInProj(projId) {
   return new Promise((resolve, reject) => {
      db = getDb();
      var myCol = db.collection(empProjColl);
      myCol.aggregate([
         {
            $lookup: {
               from: "esa_proj",
               localField: "empEsaLink",
               foreignField: "empEsaLink",
               as: "empEsaProj"
            }
         },
         {
            $unwind: "$empEsaProj"
         },
         {
            $lookup: {
               from: "wrk_loc",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $lookup: {
               from: "emp_leave",
               localField: "ctsEmpId",
               foreignField: "ctsEmpId",
               as: "empEsaLeave"
            }
         },
         {
            $unwind: "$empEsaLeave"
         },
         {
            $match: {
               "esaId": projId,
               "projectionActive": "0"
            }
         },
         {
            $group: {
               "_id": "$_id",
               "esaId": { "$first": "$empEsaProj.esaId" },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "projName": { "$first": "$projName" },
               "ctsEmpId": { "$first": "$ctsEmpId" },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "wrkCity": { "$first": "$empEsaLoc.cityName" },
               "wrkHrPerDay": { "$first": "$wrkHrPerDay" },
               "billRatePerHr": { "$first": "$billRatePerHr" },
               "empEsaLink": { "$first": "$empEsaLink" },
               "projectionActive": { "$first": "$projectionActive" },
               "leave": {
                  "$push": {
                     "_id": "$empEsaLeave._id",
                     "month": "$empEsaLeave.month",
                     "startDate": "$empEsaLeave.startDate",
                     "endDate": "$empEsaLeave.endDate",
                     "days": "$empEsaLeave.days"
                  }
               }
            }
         }
      ]).toArray(function (err, oneProj) {
         if (err) {
            reject(err);
         } else {
            resolve(oneProj);
         }
      });
   });
}




//get projection data for all projects
function listAllActiveEmployee() {
   return new Promise((resolve, reject) => {
      db = getDb();
      var myCol = db.collection(empProjColl);
      myCol.aggregate([
         {
            $lookup: {
               from: "esa_proj",
               localField: "empEsaLink",
               foreignField: "empEsaLink",
               as: "empEsaProj"
            }
         },
         {
            $unwind: "$empEsaProj"
         },
         {
            $lookup: {
               from: "wrk_loc",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $lookup: {
               from: "emp_leave",
               localField: "ctsEmpId",
               foreignField: "ctsEmpId",
               as: "empEsaLeave"
            }
         },
         {
            $unwind: "$empEsaLeave"
         },
         {
            $match: {
               "projectionActive": "1"
            }
         },
         {
            $group: {
               "_id": "$_id",
               "esaId": { "$first": "$empEsaProj.esaId" },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "projName": { "$first": "$projName" },
               "ctsEmpId": { "$first": "$ctsEmpId" },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "wrkCity": { "$first": "$empEsaLoc.cityName" },
               "wrkHrPerDay": { "$first": "$wrkHrPerDay" },
               "billRatePerHr": { "$first": "$billRatePerHr" },
               "empEsaLink": { "$first": "$empEsaLink" },
               "projectionActive": { "$first": "$projectionActive" },
               "leave": {
                  "$push": {
                     "_id": "$empEsaLeave._id",
                     "month": "$empEsaLeave.month",
                     "startDate": "$empEsaLeave.startDate",
                     "endDate": "$empEsaLeave.endDate",
                     "days": "$empEsaLeave.days"
                  }
               }
            }
         }
      ]).toArray(function (err, oneProj) {
         if (err) {
            reject(err);
         } else {
            resolve(oneProj);
         }
      });
   });
}




//get projection data for all projects
function listAllInactiveEmployee() {
   return new Promise((resolve, reject) => {
      db = getDb();
      var myCol = db.collection(empProjColl);
      myCol.aggregate([
         {
            $lookup: {
               from: "esa_proj",
               localField: "empEsaLink",
               foreignField: "empEsaLink",
               as: "empEsaProj"
            }
         },
         {
            $unwind: "$empEsaProj"
         },
         {
            $lookup: {
               from: "wrk_loc",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $lookup: {
               from: "emp_leave",
               localField: "ctsEmpId",
               foreignField: "ctsEmpId",
               as: "empEsaLeave"
            }
         },
         {
            $unwind: "$empEsaLeave"
         },
         {
            $match: {
               "projectionActive": "0"
            }
         },
         {
            $group: {
               "_id": "$_id",
               "esaId": { "$first": "$empEsaProj.esaId" },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "projName": { "$first": "$projName" },
               "ctsEmpId": { "$first": "$ctsEmpId" },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "wrkCity": { "$first": "$empEsaLoc.cityName" },
               "wrkHrPerDay": { "$first": "$wrkHrPerDay" },
               "billRatePerHr": { "$first": "$billRatePerHr" },
               "empEsaLink": { "$first": "$empEsaLink" },
               "projectionActive": { "$first": "$projectionActive" },
               "leave": {
                  "$push": {
                     "_id": "$empEsaLeave._id",
                     "month": "$empEsaLeave.month",
                     "startDate": "$empEsaLeave.startDate",
                     "endDate": "$empEsaLeave.endDate",
                     "days": "$empEsaLeave.days"
                  }
               }
            }
         }
      ]).toArray(function (err, oneProj) {
         if (err) {
            reject(err);
         } else {
            resolve(oneProj);
         }
      });
   });
}




module.exports = {
   getDb,
   initDb,
   listAllProjects,
   listEmployeeInProj,
   getEmployeeProjection,
   getAllEmployeeLeaves,
   listAllEmployees,
   listActiveEmployeeInProj,
   listInactiveEmployeeInProj,
   listAllActiveEmployee,
   listAllInactiveEmployee
};