const assert = require("assert");
const dateTime = require("date-and-time");
const dateFormat = require("dateformat");
const printf = require('printf');
const database = "invoicesdb";
const lastMonth = 11;
const firstMonth = 0;
const monthFirstDate = 1;
const monthLastDate = 0;
const esaProjColl = "esa_proj";
const empProjColl = "emp_proj";

var MongoClient = require('mongodb').MongoClient;
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


function updEmpMonthRevenue(empJsonObj, monthRevArr) {
   return new Promise((resolve, _reject) => {
      let empEsaLink = empJsonObj[0].empEsaLink;
      let ctsEmpId = empJsonObj[0].ctsEmpId;
      let month = monthRevArr.month;
      let startDate = dateFormat((new Date(dateTime.parse(monthRevArr.startDate, "DD-MMM-YYYY", true))), "ddmmyyyy");
      let endDate = dateFormat((new Date(dateTime.parse(monthRevArr.endDate, "DD-MMM-YYYY", true))), "ddmmyyyy");
      let cmiRevenue = monthRevArr.cmiRevenue;
      let monthRevenue = monthRevArr.monthRevenue;
      let empMonthRevObj = { 'empEsaLink': empEsaLink, 'ctsEmpId': ctsEmpId, 'month': month, 'startDate': startDate, 'endDate': endDate, 'monthRevenue': monthRevenue, 'cmiRevenue': cmiRevenue };
      db = getDb();
      var myCol = db.collection(empMonthRevColl);
      myCol.updateOne({ 'empEsaLink': empEsaLink, 'ctsEmpId': ctsEmpId, 'month': month }, { $set: empMonthRevObj }, { upsert: true }).then((result) => {
         resolve(result);
      });
   });
}


/* calculate number of days between */
function getDaysBetween(startDate, endDate, getWeekDays) {
   let daysBetween = 0;
   return new Promise((resolve, _reject) => {
      if (!startDate || isNaN(startDate) || !endDate || isNaN(endDate)) {
         resolve(daysBetween);
      } else {
         /* clone date to avoid messing up original data */
         var fromDate = new Date(startDate.getTime());
         var toDate = new Date(endDate.getTime());

         /* reset time */
         fromDate.setHours(0, 0, 0, 0);
         toDate.setHours(0, 0, 0, 0);

         daysBetween = 1;
         if (fromDate === toDate) {
            resolve(daysBetween);
         } else {
            while (fromDate < toDate) {
               fromDate.setDate(fromDate.getDate() + 1);
               var dayOfWeek = fromDate.getDay();
               /* check if the date is neither a Sunday(0) nor a Saturday(6) */
               if (getWeekDays === true) {
                  if (dayOfWeek > 0 && dayOfWeek < 6) {
                     daysBetween++;
                  }
               } else {
                  daysBetween++;
               }
            }
            resolve(daysBetween);
         }
      }
   });
};


function calcMonthLeaves(empLeaveArrObj, monthIndex, revMonthStartDate, revMonthEndDate) {
   let leaveDays = 0;
   return new Promise((resolve, _reject) => {
      empLeaveArrObj.forEach((leave) => {
         if (leave.startDate != 0 && !isNaN(leave.startDate) && leave.endDate != 0 && !isNaN(leave.endDate)) {
            var leaveStart = new Date(dateTime.parse(leave.startDate, "DDMMYYYY", true));
            var leaveEnd = new Date(dateTime.parse(leave.endDate, "DDMMYYYY", true));
            if (monthIndex === leaveStart.getMonth() && monthIndex !== leaveEnd.getMonth()) {
               leaveEnd = revMonthEndDate;
            } else if (monthIndex === leaveEnd.getMonth()) {
               leaveStart = revMonthStartDate;
            }

            getDaysBetween(leaveStart, leaveEnd, true).then((daysBetween) => {
               leaveDays += parseInt(daysBetween, 10);
            });
         }
      });
      resolve(leaveDays);
   });
}


function calcMonthBuffers(empBufferArrObj, monthIndex) {
   let bufferDays = 0;
   return new Promise((resolve, _reject) => {
      empBufferArrObj.forEach((buffer) => {
         let bufferMonth = new Date(dateTime.parse(buffer.month, "MMYYYY", true)).getMonth();
         if (monthIndex === bufferMonth) {
            bufferDays += parseInt(buffer.days, 10);
         }
      });
      resolve(bufferDays);
   });
}


function calcRevenueDays(empJsonObj, monthIndex, revMonthStartDate, revMonthEndDate) {
   return new Promise((resolve, _reject) => {
      getDaysBetween(revMonthStartDate, revMonthEndDate, true).then((weekDays) => {
         calcMonthLeaves(empJsonObj[0].leaves, monthIndex, revMonthStartDate, revMonthEndDate).then((personalDays) => {
            calcMonthLeaves(empJsonObj[0].publicHolidays, monthIndex, revMonthStartDate, revMonthEndDate).then((locationLeaves) => {
               resolve(weekDays - (personalDays + locationLeaves));
            });
         });
      });
   });
}


function getEmpMonthlyRevenue(empJsonObj, revenueYear, monthIndex, revMonthStartDate, revMonthEndDate) {
   return new Promise((resolve, _reject) => {
      let billRatePerHr = parseInt(empJsonObj[0].billRatePerHr, 10);
      let billHourPerDay = parseInt(empJsonObj[0].wrkHrPerDay, 10);
      let revenueMonth = printf("%02s%04s", monthIndex + 1, parseInt(revenueYear, 10));
      let revenueAmount = 0;
      let cmiRevenue = 0;
      calcRevenueDays(empJsonObj, monthIndex, revMonthStartDate, revMonthEndDate).then((revenueDays) => {
         let monthRevenueObj = "";
         cmiRevenue = printf("%10.2f", (revenueDays * billHourPerDay * billRatePerHr) / 100);
         calcMonthBuffers(empJsonObj[0].buffers, monthIndex).then((bufferDays) => {
            revenueAmount = printf("%10.2f", ((revenueDays - bufferDays) * billHourPerDay * billRatePerHr) / 100);
            monthRevenueObj = { 'month': revenueMonth, 'startDate': dateFormat(revMonthStartDate, "dd-mmm-yyyy"), 'endDate': dateFormat(revMonthEndDate, "dd-mmm-yyyy"), 'monthRevenue': revenueAmount, 'cmiRevenue': cmiRevenue };
            updEmpMonthRevenue(empJsonObj, monthRevenueObj).then((result) => {
               const { matchedCount, modifiedCount, upsertedCount } = result;
               console.log("matchedCount: " + matchedCount + "    modifiedCount: " + modifiedCount + "    upsertedCount: " + upsertedCount);
               if (matchedCount === 0 && upsertedCount >= 1) {
                  if (upsertedCount === 1) {
                     console.log("Inserted " + upsertedCount + " record");
                  } else {
                     console.log("Inserted " + upsertedCount + " records")
                  }
               } else if (matchedCount === 1 && modifiedCount >= 1) {
                  if (modifiedCount === 1) {
                     console.log("Updated " + modifiedCount + " record");
                  } else {
                     console.log("Updated " + modifiedCount + " records");
                  }
               } else {
                  console.log("Nothing matches !");
               }
            });
            resolve(monthRevenueObj);
         });
      });
   });
}


/*
   listEmployeeInProj:
      to get list of employees in a project

   pre-requisite:
      requires getEmployeeProjection to be called first

   params:
      empJsonObj - employee detail object
      revenueYear - year for which leaves are required

   returns an array with yearly revenue details
*/
function calcEmpRevenue(empJsonObj, revenueYear) {
   let monthRevArr = [];
   return new Promise((resolve, reject) => {
      if (!empJsonObj || isNaN(empJsonObj)) {
         reject("Employee object is not provided");
      } else if (!empJsonObj[0].sowStartDate || isNaN(empJsonObj[0].sowStartDate)) {
         reject("SOW start date is not defined for selected employee");
      } else if (!empJsonObj[0].sowEndDate || isNaN(empJsonObj[0].sowEndDate)) {
         reject("SOW end date is not defined for selected employee");
      } else if (!empJsonObj[0].billRatePerHr || isNaN(empJsonObj[0].billRatePerHr)) {
         reject("Billing hour per day is not defined for selected employee");
      } else if (!empJsonObj[0].wrkHrPerDay || isNaN(empJsonObj[0].wrkHrPerDay)) {
         reject("Billing rate is not defined for selected employee");
      } else if (!revenueYear || isNaN(revenueYear)) {
         reject("Revenue year is not provided");
      }
      let sowStart = new Date(dateTime.parse(empJsonObj[0].sowStartDate, "DDMMYYYY", true));
      let sowEnd = new Date(dateTime.parse(empJsonObj[0].sowEndDate, "DDMMYYYY", true));

      for (let monthIndex = firstMonth; monthIndex <= lastMonth; monthIndex++) {
         let revMonthStartDate = new Date(revenueYear, monthIndex, monthFirstDate);
         let revMonthEndDate = new Date(revenueYear, monthIndex + 1, monthLastDate);
         if (sowStart.getFullYear() === parseInt(revenueYear, 10)) {
            if (sowStart.getMonth() === monthIndex) {
               revMonthStartDate = sowStart;
            }
         }

         if (sowEnd.getFullYear() === parseInt(revenueYear, 10)) {
            if (sowEnd.getMonth() === monthIndex) {
               revMonthEndDate = sowEnd;
            }
         }
         if (revMonthStartDate >= sowStart && revMonthEndDate <= sowEnd) {
            monthRevArr.push(getEmpMonthlyRevenue(empJsonObj, revenueYear, monthIndex, revMonthStartDate, revMonthEndDate));
         } else {
            let revenueMonth = printf("%02s%04s", monthIndex + 1, parseInt(revenueYear, 10));
            let monthRevenueObj = { 'month': revenueMonth, 'startDate': dateFormat(revMonthStartDate, "dd-mmm-yyyy"), 'endDate': dateFormat(revMonthEndDate, "dd-mmm-yyyy"), 'monthRevenue': "0.00", 'cmiRevenue': "0.00" };
            monthRevArr.push(monthRevenueObj);
         }
      }
      Promise.all(monthRevArr).then((monthRevenue) => {
         resolve(monthRevenue);
      });
   });
}


/* get list of projects */
function listAllProjects() {
   return new Promise((resolve, reject) => {
      db = getDb();
      db.collection(esaProjColl).aggregate([
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


/*
   listEmployeeInProj:
      to get list of employees in a project

   params:
      empEsaLink - linker between employee and project
      ctsEmpId - employee id
      revenueYear - year for which leaves are required
      personal - boolean flag
         if true, will fetch personal leaves
         else, will fetch location holidays

   returns an array with leave details
*/
function listEmployeeInProj(esaId) {
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
               "esaId": esaId
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



function getProjectRevenue(esaId, revenueYear) {
   let empDtlArr = [];
   return new Promise((resolve, _reject) => {
      listEmployeeInProj(esaId).then((empInProj) => {
         empInProj.forEach((employee) => {
            getEmployeeProjection(employee.empEsaLink, employee.ctsEmpId, revenueYear).then((empDtl) => {
               console.log(empDtl);
               empDtlArr.push(empDtl);
            });
         });
      });
      Promise.all(empDtlArr).then((allEmpDtl) => {
         resolve(allEmpDtl);
      })
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
function listActiveEmployeeInProj(esaId) {
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
               "esaId": esaId,
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
function listInactiveEmployeeInProj(esaId) {
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
               "esaId": esaId,
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
   getAllEmployeeLeaves,
   listAllEmployees,
   listActiveEmployeeInProj,
   listInactiveEmployeeInProj,
   listAllActiveEmployee,
   listAllInactiveEmployee,
   getProjectRevenue,
   calcEmpRevenue
};