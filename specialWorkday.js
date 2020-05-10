const dbObj = require("./database");
const commObj = require("./utility");
const empSplWrkObj = "emp_spl_wrk";
const locSplWrkObj = "loc_spl_wrk";
const leaveHour = 4;
const mSecInDay = 86400000;


function getEmpSplWrk(empEsaLink, ctsEmpId, splWrkStart, splWrkStop, callerName) {
   let funcName = getEmpSplWrk.name;
   return new Promise((resolve, reject) => {
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(callerName + " -> " + funcName + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(callerName + " -> " + funcName + ": Employee ID is not provided");
      } else if (splWrkStart === undefined || splWrkStart === "") {
         reject(callerName + " -> " + funcName + ": Leave start date is not provided");
      } else if (splWrkStop === undefined || splWrkStop === "") {
         reject(callerName + " -> " + funcName + ": Leave stop date is not provided");
      } else {
         let refSplWrkStart = new Date(splWrkStart);
         refSplWrkStart.setUTCHours(0, 0, 0, 0);
         let refSplWrkStop = new Date(splWrkStop);
         refSplWrkStop.setUTCHours(0, 0, 0, 0);
         dbObj.getDb().collection(empSplWrkObj).aggregate([
            {
               $project: {
                  "_id": "$_id",
                  "empEsaLink": "$empEsaLink",
                  "ctsEmpId": "$ctsEmpId",
                  "startDate": "$startDate",
                  "stopDate": "$stopDate",
                  "reason": "$reason",
                  "workStart": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$startDate", 4, -1] } },
                        month: { $toInt: { $substr: ["$startDate", 2, 2] } },
                        day: { $toInt: { $substr: ["$startDate", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0
                     }
                  },
                  "workStop": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$stopDate", 4, -1] } },
                        month: { $toInt: { $substr: ["$stopDate", 2, 2] } },
                        day: { $toInt: { $substr: ["$stopDate", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0
                     }
                  }
               }
            },
            {
               $match: {
                  "empEsaLink": empEsaLink,
                  "ctsEmpId": ctsEmpId,
                  $or: [
                     {
                        $and: [
                           { "workStart": { $lte: refSplWrkStart } },
                           { "workStop": { $gte: refSplWrkStop } }
                        ]
                     },
                     {
                        $and: [
                           { "workStart": { $lte: refSplWrkStart } },
                           { "workStop": { $gte: refSplWrkStart } },
                           { "workStop": { $lte: refSplWrkStop } }
                        ]
                     },
                     {
                        $and: [
                           { "workStart": { $gte: refSplWrkStart } },
                           { "workStart": { $lte: refSplWrkStop } },
                           { "workStop": { $gte: refSplWrkStart } },
                           { "workStop": { $lte: refSplWrkStop } }
                        ]
                     },
                     {
                        $and: [
                           { "workStart": { $gte: refSplWrkStart } },
                           { "workStart": { $lte: refSplWrkStop } },
                           { "workStop": { $gte: refSplWrkStart } },
                           { "workStop": { $lte: refSplWrkStop } }
                        ]
                     }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$_id",
                  "startDate": "$workStart",
                  "stopDate": "$workStop",
                  "days": { $divide: [{ $add: [{ $subtract: ["$workStop", "$workStart"] }, mSecInDay] }, mSecInDay] },
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
                  stopDate: {
                     $let: {
                        vars: {
                           monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                        },
                        in: {
                           $concat: [
                              { $toString: { $dayOfMonth: "$stopDate" } }, "-",
                              { $arrayElemAt: ["$$monthsInString", { $month: "$stopDate" }] }, "-",
                              { $toString: { $year: "$stopDate" } }
                           ]
                        }
                     }
                  }
               }
            }
         ]).toArray((err, empSplWrkArr) => {
            if (err) {
               reject("DB error in " + funcName + ": " + err);
            } else if (empSplWrkArr.length >= 1) {
               commObj.countAllDays(empSplWrkArr, funcName).then((allDaysInLeave) => {
                  commObj.countWeekdays(empSplWrkArr, funcName).then((workDaysInLeave) => {
                     empSplWrkArr.push({ "totalDays": allDaysInLeave, "workDays": workDaysInLeave });
                     resolve(empSplWrkArr);
                  });
               });
            } else {
               resolve(empSplWrkArr);
            }
         });
      }
   });
}


function getLocSplWrk(cityCode, splWrkStart, splWrkStop, callerName) {
   let funcName = getLocSplWrk.name;
   return new Promise((resolve, reject) => {
      if (cityCode === undefined || cityCode === "") {
         reject(callerName + " -> " + funcName + ": City code is not provided");
      } else if (splWrkStart === undefined || splWrkStart === "") {
         reject(callerName + " -> " + funcName + ": Leave start date is not provided");
      } else if (splWrkStop === undefined || splWrkStop === "") {
         reject(callerName + " -> " + funcName + ": Leave stop date is not provided");
      } else {
         let refSplWrkStart = new Date(splWrkStart);
         refSplWrkStart.setUTCHours(0, 0, 0, 0);
         let refSplWrkStop = new Date(splWrkStop);
         refSplWrkStop.setUTCHours(0, 0, 0, 0);
         dbObj.getDb().collection(locSplWrkObj).aggregate([
            {
               $project: {
                  "_id": "$_id",
                  "cityCode": "$cityCode",
                  "startDate": "$startDate",
                  "stopDate": "$stopDate",
                  "reason": "$reason",
                  "workStart": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$startDate", 4, -1] } },
                        month: { $toInt: { $substr: ["$startDate", 2, 2] } },
                        day: { $toInt: { $substr: ["$startDate", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0
                     }
                  },
                  "workStop": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$stopDate", 4, -1] } },
                        month: { $toInt: { $substr: ["$stopDate", 2, 2] } },
                        day: { $toInt: { $substr: ["$stopDate", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0
                     }
                  }
               }
            },
            {
               $match: {
                  "cityCode": cityCode,
                  $or: [
                     {
                        $and: [
                           { "workStart": { $lte: refSplWrkStart } },
                           { "workStop": { $gte: refSplWrkStop } }
                        ]
                     },
                     {
                        $and: [
                           { "workStart": { $lte: refSplWrkStart } },
                           { "workStop": { $gte: refSplWrkStart } },
                           { "workStop": { $lte: refSplWrkStop } }
                        ]
                     },
                     {
                        $and: [
                           { "workStart": { $gte: refSplWrkStart } },
                           { "workStart": { $lte: refSplWrkStop } },
                           { "workStop": { $gte: refSplWrkStart } },
                           { "workStop": { $lte: refSplWrkStop } }
                        ]
                     },
                     {
                        $and: [
                           { "workStart": { $gte: refSplWrkStart } },
                           { "workStart": { $lte: refSplWrkStop } },
                           { "workStop": { $gte: refSplWrkStart } },
                           { "workStop": { $lte: refSplWrkStop } }
                        ]
                     }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$_id",
                  "startDate": "$workStart",
                  "stopDate": "$workStop",
                  "days": { $divide: [{ $add: [{ $subtract: ["$workStop", "$workStart"] }, mSecInDay] }, mSecInDay] },
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
                  stopDate: {
                     $let: {
                        vars: {
                           monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                        },
                        in: {
                           $concat: [
                              { $toString: { $dayOfMonth: "$stopDate" } }, "-",
                              { $arrayElemAt: ["$$monthsInString", { $month: "$stopDate" }] }, "-",
                              { $toString: { $year: "$stopDate" } }
                           ]
                        }
                     }
                  }
               }
            }
         ]).toArray((err, locSplWrkArr) => {
            if (err) {
               reject("DB error in " + getLocSplWrk.name + ": " + err);
            } else if (locSplWrkArr.length >= 1) {
               commObj.countAllDays(locSplWrkArr, funcName).then((allDaysInLeave) => {
                  commObj.countWeekdays(locSplWrkArr, funcName).then((workDaysInLeave) => {
                     locSplWrkArr.push({ "totalDays": allDaysInLeave, "workDays": workDaysInLeave });
                     resolve(locSplWrkArr);
                  });
               });
            } else {
               resolve(locSplWrkArr);
            }
         });
      }
   });
}


function getWeekEndHours(empSplWrkArr, locSplWrkArr, _callerName) {
   /*let funcName = getWeekEndHours.name;*/
   return new Promise(async (resolve, _reject) => {
      if (empSplWrkArr === undefined || empSplWrkArr === "" ||
         locSplWrkArr === undefined || locSplWrkArr === "") {
         resolve(0);
      } else {
         let weekendHrs = 0;
         if (empSplWrkArr.length > 0 || locSplWrkArr.length > 0) {
            empSplWrkArr.forEach(async (empSplWrk) => {
               let empSplStart = new Date(empSplWrk.startDate);
               empSplStart.setUTCHours(0, 0, 0, 0);
               let empSplStop = new Date(empSplWrk.stopDate);
               empSplStop.setUTCHours(0, 0, 0, 0);
               let fromDate = empSplStart;

               await locSplWrkArr.forEach((locSplWrk) => {
                  let locSplStart = new Date(locSplWrk.startDate);
                  locSplStart.setUTCHours(0, 0, 0, 0);
                  let locSplStop = new Date(locSplWrk.stopDate);
                  locSplStop.setUTCHours(0, 0, 0, 0);

                  if ((empSplStart.getTime() <= locSplStart.getTime() && locSplStart.getTime() <= empSplStop.getTime()) ||
                     (empSplStart.getTime() <= locSplStop.getTime() && locSplStop.getTime() <= empSplStop.getTime()) ||
                     ((locSplStart.getTime() < empSplStart.getTime() && empSplStop.getTime() < locSplStop.getTime()))) {
                     /* there is a overlap */
                     while (fromDate.getTime() <= empSplStop.getTime()) {
                        if (fromDate.getDay() === 0 || fromDate.getDay() === 6) {
                           if (fromDate.getTime() >= locSplStart.getTime() && fromDate.getTime() <= locSplStop.getTime()) {
                              if (empSplWrk.halfDay === "Y" && locSplWrk.halfDay === "Y") {
                                 weekendHrs += billHrPerDay;
                              } else if (empSplWrk.halfDay === "Y" || locSplWrk.halfDay === "Y") {
                                 weekendHrs += billHrPerDay - leaveHour;
                              } else {
                                 weekendHrs += billHrPerDay;
                              }
                           } else if (fromDate.halfDay === "Y") {
                              weekendHrs += (billHrPerDay - leaveHour);
                           } else {
                              weekendHrs += billHrPerDay;
                           }
                        }
                        fromDate.setDate(fromDate.getDate() + 1);
                     }
                  }
               });
            });
            resolve(weekendHrs);
         } else {
            resolve(0);
         }
      }
   });
}


function getSplWrkWkEndHrs(empEsaLink, ctsEmpId, cityCode, billHrPerDay, splWrkStart, splWrkStop, callerName) {
   let funcName = getSplWrkWkEndHrs.name;
   return new Promise((resolve, reject) => {
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(callerName + " -> " + funcName + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(callerName + " -> " + funcName + ": Employee ID is not provided");
      } else if (cityCode === undefined || cityCode === "") {
         reject(callerName + " -> " + funcName + ": City code is not provided");
      } else if (billHrPerDay === undefined || billHrPerDay === "") {
         reject(callerName + " -> " + funcName + ": Billing hour per day is not provided");
      } else if (splWrkStart === undefined || splWrkStart === "") {
         reject(callerName + " -> " + funcName + ": Leave start date is not provided");
      } else if (splWrkStop === undefined || splWrkStop === "") {
         reject(callerName + " -> " + funcName + ": Leave stop date is not provided");
      } else {
         getEmpSplWrk(empEsaLink, ctsEmpId, splWrkStart, splWrkStop, funcName).then((empSplWrkArr) => {
            getLocSplWrk(cityCode, splWrkStart, splWrkStop, getSplWrkWkEndHrs.name).then((locSplWrkArr) => {
               getWeekEndHours(empSplWrkArr, locSplWrkArr, getSplWrkWkEndHrs.name).then((weekEndHours) => {
                  resolve(weekEndHours);
               });
            }).catch((getLocSplWrkErr) => { reject(getLocSplWrkErr) });
         }).catch((getEmpSplWrkErr) => { reject(getEmpSplWrkErr) });
      }
   });
}


function getSplWrkDays(empEsaLink, ctsEmpId, cityCode, splWrkStart, splWrkStop, callerName) {
   let funcName = getSplWrkDays.name;
   return new Promise((resolve, reject) => {
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(callerName + " -> " + funcName + ": ESA Linker ID not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(callerName + " -> " + funcName + ": Employee ID is not provided");
      } else if (cityCode === undefined || cityCode === "") {
         reject(callerName + " -> " + funcName + ": Work city code is not provided");
      } else if (splWrkStart === undefined || splWrkStart === "") {
         reject(callerName + " -> " + funcName + ": Start date is not provided");
      } else if (splWrkStop === undefined || splWrkStop === "") {
         reject(callerName + " -> " + funcName + ": Stop date is not provided");
      } else {
         getEmpSplWrk(empEsaLink, ctsEmpId, splWrkStart, splWrkStop, funcName).then((empSplWrkArr) => {
            getLocSplWrk(cityCode, splWrkStart, splWrkStop, funcName).then((locSplWrkArr) => {
               let splWrkArr = { "empSplWrk": empSplWrkArr, "locSplWrk": locSplWrkArr };
               resolve(splWrkArr);
            }).catch((getLocSplWrkErr) => { reject(getLocSplWrkErr) });
         }).catch((getEmpSplWrkErr) => { reject(getEmpSplWrkErr) });
      }
   });
}


module.exports = {
   getSplWrkWkEndHrs,
   getSplWrkDays
}