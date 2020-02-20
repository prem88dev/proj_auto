const assert = require("assert");
const dateTime = require("date-and-time");
const dateFormat = require("dateformat");
const printf = require('printf');
var MongoClient = require('mongodb').MongoClient;

const database = "invoicesdb";
const empProjColl = "emp_proj";
const esaProjColl = "esa_proj";
const locLeaveColl = "loc_holiday";
const empMonthRevColl = "emp_month_revenue"
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


function updEmpMonthRevenue(empJsonObj, monthRevArr) {
   return new Promise((resolve, _reject) => {
      let empEsaLink = empJsonObj[0].empEsaLink;
      let ctsEmpId = empJsonObj[0].ctsEmpId;
      let month = monthRevArr.month;
      let startDate = dateFormat((new Date(dateTime.parse(monthRevArr.startDate, "DD-MMM-YYYY", true))), "DDMMYYYY");
      let endDate = dateFormat((new Date(dateTime.parse(monthRevArr.endDate, "DD-MMM-YYYY", true))), "DDMMYYYY");
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


/* calculate revenue */
function calcEmpRevenue(empJsonObj, revenueYear) {
   let monthRevArr = [];
   return new Promise((resolve, reject) => {
      if (!empJsonObj[0].sowStartDate || isNaN(empJsonObj[0].sowStartDate)) {
         reject("SOW start date is not defined for selected employee");
      } else if (!empJsonObj[0].sowEndDate || isNaN(empJsonObj[0].sowEndDate)) {
         reject("SOW end date is not defined for selected employee");
      } else if (!empJsonObj[0].billRatePerHr || isNaN(empJsonObj[0].billRatePerHr)) {
         reject("Billing hour per day is not defined for selected employee");
      } else if (!empJsonObj[0].wrkHrPerDay || isNaN(empJsonObj[0].wrkHrPerDay)) {
         reject("Billing rate is not defined for selected employee");
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
      let myCol = db.collection(esaProjColl);
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
            empDtl.push({ "revenue": revenueDetail });
            if (err) {
               reject(err);
            } else {
               resolve(empDtl);
            }
         });
      });
   });
}


function getProjectRevenue(projJsonObj, revenueYear) {
   return new Promise((resolve, reject) => {
      let monthRevenueArr = [];
      listEmployeeInProj(projJsonObj[0].esaId).then((empInProj) => {
         let cmiRevenue = 0;
         let monthRevenue = 0;
         getEmployeeProjection(employee.empEsaLink, employee.ctsEmpId, revenueYear).then((empDtl) => {
            for (let monthIndex = firstMonth; monthIndex <= lastMonth; monthIndex++) {
               let revenueMonth = printf("%02s%04s", monthIndex + 1, parseInt(revenueYear, 10));
               empInProj.forEach((employee) => {
                  employee[0].revenue.forEach((monthRev) => {
                     if (revenueMonth === monthRev.month) {
                        cmiRevenue += parseFloat(montRev.cmiRevenue);
                        monthRevenue += parseFloat(monthRev.monthRevenue);
                     }
                  });
               });
            }
            let monthRevenueObj = { 'month': revenueMonth, 'cmiRevenue': cmiRevenue, 'monthRevenue': monthRevenue };
            monthRevenueArr.push(monthRevenueObj);
         });
         resolve(monthRevenueArr);
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