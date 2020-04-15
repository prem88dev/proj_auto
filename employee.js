const dbObj = require("./database");
const locObj = require("./location");
const commObj = require("./utility");
const revObj = require("./revenue");
const ObjectId = require("mongodb").ObjectID;
const dateFormat = require("dateformat");
const empLeaveColl = "emp_leave";
const empBuffer = "emp_buffer";
const empProjColl = "emp_proj";
const mSecInDay = 86400000;


function computeBufferDays(bufferArr) {
   let bufferDays = 0;
   return new Promise(async (resolve, _reject) => {
      await bufferArr.forEach((buffer) => {
         bufferDays += parseInt(buffer.days, 10);
      });
      resolve(bufferDays);
   });
}


/*
   listEmployeeInProj:
      to get list of employees in a project

   params:
      esaId - project id for which employees are to be listed
   
   returns array of employee with their first, middle and last names
*/
function listEmployeeInProj(esaId) {
   return new Promise((resolve, reject) => {
      db = dbObj.getDb();
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
            $match: {
               "esaId": esaId
            }
         },
         {
            $group: {
               "_id": "$_id",
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" }
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


/*
   getPersonalLeave:
      returns an array of personal leaves

   params:
      input:
         empEsaLink - linker between employee and project
         ctsEmpId - employee id
         revenueYear - year for which leaves are required
*/
function getPersonalLeave(empEsaLink, ctsEmpId, revenueYear) {
   return new Promise((resolve, reject) => {
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(getPersonalLeave.name + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(getPersonalLeave.name + ": Employee ID is not provided");
      } else if (revenueYear === undefined || revenueYear === "") {
         reject(getPersonalLeave.name + ": Revenue year is not provided");
      }
      let leaveYear = parseInt(revenueYear, 10);
      let revenueStart = new Date(leaveYear, 0, 2);
      revenueStart.setUTCHours(0, 0, 0, 0);
      let revenueEnd = new Date(leaveYear, 12, 1);
      revenueEnd.setUTCHours(0, 0, 0, 0);
      dbObj.getDb().collection(empLeaveColl).aggregate([
         {
            $project: {
               "_id": 1,
               "esaId": 2,
               "empEsaLink": 3,
               "ctsEmpId": 4,
               "startDate": 5,
               "endDate": 6,
               "days": 7,
               "reason": 8,
               "leaveStart": {
                  $dateFromParts: {
                     year: { $toInt: { $substr: ["$startDate", 4, -1] } },
                     month: { $toInt: { $substr: ["$startDate", 2, 2] } },
                     day: { $toInt: { $substr: ["$startDate", 0, 2] } },
                     hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                  }
               },
               "leaveEnd": {
                  $dateFromParts: {
                     year: { $toInt: { $substr: ["$endDate", 4, -1] } },
                     month: { $toInt: { $substr: ["$endDate", 2, 2] } },
                     day: { $toInt: { $substr: ["$endDate", 0, 2] } },
                     hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                  }
               }
            }
         },
         {
            $match: {
               "empEsaLink": empEsaLink,
               "ctsEmpId": ctsEmpId,
               "$or": [
                  {
                     "$and": [
                        { "leaveStart": { "$lte": revenueStart } },
                        { "leaveEnd": { "$gte": revenueEnd } }
                     ]
                  },
                  {
                     "$and": [
                        { "leaveStart": { "$lte": revenueStart } },
                        { "leaveEnd": { "$gte": revenueStart } },
                        { "leaveEnd": { "$lte": revenueEnd } }
                     ]
                  },
                  {
                     "$and": [
                        { "leaveStart": { "$gte": revenueStart } },
                        { "leaveStart": { "$lte": revenueEnd } },
                        { "leaveEnd": { "$gte": revenueStart } },
                        { "leaveEnd": { "$lte": revenueEnd } }
                     ]
                  }
               ]
            }
         },
         {
            $project: {
               "_id": "$_id",
               "startDate": "$leaveStart",
               "endDate": "$leaveEnd",
               "days": {
                  $switch: {
                     branches: [
                        { case: { "$eq": ["$leaveStart", "$leaveEnd"] }, "then": 1 },
                        {
                           case: { "$gte": ["$leaveStart", "$leaveEnd"] }, "then": {
                              $divide: [{ $subtract: ["$leaveEnd", "$leaveStart"] }, mSecInDay]
                           }
                        }
                     ]
                  }
               },
               "reason": "$reason"
            }
         },
         {
            $addFields: {
               startDate: {
                  $let: {
                     vars: {
                        monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                     },
                     in: {
                        $concat: [
                           { $toString: { $dayOfMonth: "$startDate" } }, "-",
                           { $arrayElemAt: ["$$monthsInString", { $month: "$startDate" }] }, "-",
                           { $toString: { $year: "$startDate" } }
                        ]
                     }
                  }
               },
               endDate: {
                  $let: {
                     vars: {
                        monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                     },
                     in: {
                        $concat: [
                           { $toString: { $dayOfMonth: "$endDate" } }, "-",
                           { $arrayElemAt: ["$$monthsInString", { $month: "$startDate" }] }, "-",
                           { $toString: { $year: "$endDate" } }
                        ]
                     }
                  }
               }
            }
         }
      ]).toArray((err, leaveArr) => {
         if (err) {
            reject("DB error in " + getPersonalLeave.name + ": " + err);
         } else if (leaveArr.length >= 1) {
            commObj.computeLeaveDays(leaveArr).then((allDaysInLeave) => {
               commObj.computeWeekdaysInLeave(leaveArr).then((workDaysInLeave) => {
                  leaveArr.push({ "totalDays": allDaysInLeave, "workDays": workDaysInLeave });
                  resolve(leaveArr);
               });
            });
         }
         else {
            resolve(leaveArr);
         }
      });
   });
}

/*
   getBuffer:
      this function will return array of buffers

   params:
      input:
         empEsaLink - inker between employee and project
         ctsEmpId - employee id
         revenueYear - year for which leaves are required
*/
function getBuffer(empEsaLink, ctsEmpId, revenueYear) {
   return new Promise((resolve, reject) => {
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(getBuffer.name + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(getBuffer.name + ": Employee ID is not provided");
      } else if (revenueYear === undefined || revenueYear === "") {
         reject(getBuffer.name + ": Revenue year is not provided");
      }
      let bufferYear = parseInt(revenueYear, 10);
      let revenueStart = new Date(bufferYear, 0, 1);
      revenueStart.setUTCHours(0, 0, 0, 0);
      let revenueEnd = new Date(bufferYear, 12, 0);
      revenueEnd.setUTCHours(0, 0, 0, 0);
      dbObj.getDb().collection(empBuffer).aggregate([
         {
            $project: {
               "_id": 1,
               "empEsaLink": 3,
               "ctsEmpId": 4,
               "month": 5,
               "days": 6,
               "reason": 7,
               "bufferDate": {
                  $dateFromParts: {
                     year: { $toInt: { $substr: ["$month", 2, -1] } },
                     month: { $toInt: { $substr: ["$month", 0, 2] } },
                     day: 1, hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                  }
               }
            }
         },
         {
            $match: {
               "$and": [
                  { "empEsaLink": { "$eq": empEsaLink } },
                  { "ctsEmpId": { "$eq": ctsEmpId } },
                  { "bufferDate": { "$gte": revenueStart } },
                  { "bufferDate": { "$lte": revenueEnd } }
               ]
            }
         },
         {
            $project: {
               "_id": "$_id",
               "month": "$month",
               "days": { $toInt: "$days" },
               "reason": "$reason"
            }
         },
         {
            $addFields: {
               month: {
                  $let: {
                     vars: {
                        monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                     },
                     in: {
                        $concat: [
                           { $arrayElemAt: ["$$monthsInString", { $toInt: { $substr: ["$month", 0, 2] } }] }, "-",
                           { $substr: ["$month", 2, - 1] }
                        ]
                     }
                  }
               }
            }
         }
      ]).toArray((err, bufferArr) => {
         if (err) {
            reject("DB error in " + getBuffer.name + "function: " + err);
         } else if (bufferArr.length >= 1) {
            computeBufferDays(bufferArr).then((bufferDays) => {
               bufferArr.push({ "totalDays": bufferDays });
               resolve(bufferArr);
            });
         }
         else {
            resolve(bufferArr);
         }
      });
   });
}


/* get projection data for specific employee in selected project */
function getEmployeeProjection(recordId, revenueYear) {
   return new Promise((resolve, reject) => {
      if (revenueYear === undefined || revenueYear === "") {
         reject(getEmployeeProjection.name + ": Revenue year is not provided");
      } else if (recordId === undefined || recordId === "") {
         reject(getEmployeeProjection.name + ": Record id is not provided");
      }

      let recordObjId = new ObjectId(recordId);
      let revenueStart = new Date(revenueYear, 0, 2);
      revenueStart.setUTCHours(0, 0, 0, 0);
      let revenueEnd = new Date(revenueYear, 12, 1);
      revenueEnd.setUTCHours(0, 0, 0, 0);
      db = dbObj.getDb();
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
               localField: "cityCode",
               foreignField: "cityCode",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $match: {
               "_id": recordObjId
            }
         },
         {
            $group: {
               "_id": "$_id",
               "esaId": { "$first": { $toInt: "$empEsaProj.esaId" } },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "projName": { "$first": "$projName" },
               "ctsEmpId": { "$first": { $toInt: "$ctsEmpId" } },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "cityCode": { "$first": "$empEsaLoc.cityCode" },
               "cityName": { "$first": "$empEsaLoc.cityName" },
               "wrkHrPerDay": { "$first": { $toInt: "$wrkHrPerDay" } },
               "billRatePerHr": { "$first": { $toInt: "$billRatePerHr" } },
               "currency": { "$first": "$empEsaProj.currency" },
               "empEsaLink": { "$first": "$empEsaLink" }
            }
         },
         {
            $addFields: {
               sowStartDate: {
                  $cond: {
                     if: { $ne: ["$sowStartDate", ""] }, then: {
                        $let: {
                           vars: {
                              monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                           },
                           in: {
                              $concat: [
                                 { $toString: { $toInt: { $substr: ["$sowStartDate", 0, 2] } } }, "-",
                                 { $arrayElemAt: ["$$monthsInString", { $toInt: { $substr: ["$sowStartDate", 2, 2] } }] }, "-",
                                 { $substr: ["$sowStartDate", 4, -1] }
                              ]
                           }
                        }
                     }, else: "$sowStartDate"
                  }
               },
               sowEndDate: {
                  $cond: {
                     if: { $ne: ["$sowEndDate", ""] }, then: {
                        $let: {
                           vars: {
                              monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                           },
                           in: {
                              $concat: [
                                 { $toString: { $toInt: { $substr: ["$sowEndDate", 0, 2] } } }, "-",
                                 { $arrayElemAt: ["$$monthsInString", { $toInt: { $substr: ["$sowEndDate", 2, 2] } }] }, "-",
                                 { $substr: ["$sowEndDate", 4, -1] }
                              ]
                           }
                        }
                     }, else: "$sowEndDate"
                  }
               },
               foreseenEndDate: {
                  $cond: {
                     if: { $ne: ["$foreseenEndDate", ""] }, then: {
                        $let: {
                           vars: {
                              monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                           },
                           in: {
                              $concat: [
                                 { $toString: { $toInt: { $substr: ["$foreseenEndDate", 0, 2] } } }, "-",
                                 { $arrayElemAt: ["$$monthsInString", { $toInt: { $substr: ["$foreseenEndDate", 2, 2] } }] }, "-",
                                 { $substr: ["$foreseenEndDate", 4, -1] }
                              ]
                           }
                        }
                     }, else: "$foreseenEndDate"
                  }
               }
            }
         }
      ]).toArray((err, empDtl) => {
         if (err) {
            reject("DB error in " + getEmployeeProjection.name + " function: " + err);
         } else if (empDtl.length === 1) {
            let empEsaLink = empDtl[0].empEsaLink;
            let ctsEmpId = `${empDtl[0].ctsEmpId}`;
            let cityCode = empDtl[0].cityCode;
            getPersonalLeave(empEsaLink, ctsEmpId, revenueYear).then((leaveArr) => {
               empDtl.push({ "leaves": leaveArr });
               getBuffer(empEsaLink, ctsEmpId, revenueYear).then((bufferArr) => {
                  empDtl.push({ "buffers": bufferArr });
                  locObj.getLocationLeave(cityCode, revenueYear).then((pubLeaveArr) => {
                     empDtl.push({ "publicHolidays": pubLeaveArr });
                     revObj.calcEmpRevenue(empDtl, revenueYear).then((revenueArr) => {
                        empDtl.push({ "revenue": revenueArr });
                        if (leaveArr.length === 0) {
                           empDtl[1].leaves.push("No leaves between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueEnd, "d-mmm-yyyy"));
                        }
                        if (bufferArr.length === 0) {
                           empDtl[2].buffers.push("No buffers between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueEnd, "d-mmm-yyyy"));
                        }
                        if (pubLeaveArr.length === 0) {
                           empDtl[3].publicHolidays.push("No location holidays between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueEnd, "d-mmm-yyyy"));
                        }
                        resolve(empDtl);
                     }).catch((calcEmpRevenueErr) => { reject(calcEmpRevenueErr); });
                  }).catch((getLocationLeaveErr) => { reject(getLocationLeaveErr); });
               }).catch((getBufferErr) => { reject(getBufferErr); });
            }).catch((getPersonalLeaveErr) => { reject(getPersonalLeaveErr); });
         } else if (empDtl.length === 0) {
            reject(getEmployeeProjection.name + ": No records found")
         } else if (empDtl.length > 1) {
            reject(getEmployeeProjection.name + ": More than one record found");
         }
      });
   });
}


module.exports = {
   listEmployeeInProj,
   getPersonalLeave,
   getBuffer,
   getEmployeeProjection
}