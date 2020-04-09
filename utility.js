const dbObj = require("./database");
const empLeaveColl = "emp_leave";
const locLeaveColl = "loc_holiday";

function computeWeekdaysInLeave(leaveArr) {
   let weekdaysInLeave = 0;
   return new Promise(async (resolve, _reject) => {
      await leaveArr.forEach((leave) => {
         getDaysBetween(leave.startDate, leave.endDate, true).then((weekdays) => {
            weekdaysInLeave += weekdays;
         });
      });
      resolve(weekdaysInLeave);
   });
}

function computeLeaveDays(leaveArr) {
   let leaveDays = 0;
   return new Promise(async (resolve, _reject) => {
      await leaveArr.forEach((leave) => {
         getDaysBetween(leave.startDate, leave.endDate, false).then((daysBetween) => {
            leaveDays += daysBetween;
         });
      });
      resolve(leaveDays);
   });
}

/* calculate number of days between */
function getDaysBetween(startDate, endDate, getWeekDays) {
   let daysBetween = 0;
   return new Promise((resolve, _reject) => {
      if (startDate === undefined || endDate === undefined) {
         resolve(daysBetween);
      } else {
         /* clone date to avoid messing up original data */
         let fromDate = new Date(startDate);
         let toDate = new Date(endDate);

         fromDate.setHours(0, 0, 0, 0);
         toDate.setHours(0, 0, 0, 0);

         if (fromDate.getTime() === toDate.getTime()) {
            let dayOfWeek = fromDate.getDay();
            if (getWeekDays === true) {
               if (dayOfWeek > 0 && dayOfWeek < 6) {
                  daysBetween++;
               }
            } else {
               daysBetween++;
            }
         } else {
            while (fromDate <= toDate) {
               let dayOfWeek = fromDate.getDay();
               /* check if the date is neither a Sunday(0) nor a Saturday(6) */
               if (getWeekDays === true) {
                  if (dayOfWeek > 0 && dayOfWeek < 6) {
                     daysBetween++;
                  }
               } else {
                  daysBetween++;
               }
               fromDate.setDate(fromDate.getDate() + 1);
            }
         }
         resolve(daysBetween);
      }
   });
};


function countPersonalDays(empEsaLink, ctsEmpId, leaveStart, leaveStop) {
   return new Promise((resolve, reject) => {
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(getPersonalLeave.name + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(getPersonalLeave.name + ": Employee ID is not provided");
      } else if (leaveStart === undefined || leaveStart === "") {
         reject(getPersonalLeave.name + ": Leave start date is not provided");
      } else if (leaveStop === undefined || leaveStop === "") {
         reject(getPersonalLeave.name + ": Leave end date is not provided");
      } else {
         let leaveBegin = new Date(leaveStart);
         leaveBegin.setHours(0, 0, 0, 0);
         let leaveDone = new Date(leaveStop);
         leaveDone.setHours(0, 0, 0, 0);
         dbObj.getDb().collection(empLeaveColl).aggregate([
            {
               $project: {
                  "_id": 1,
                  "empEsaLink": 3,
                  "ctsEmpId": 4,
                  "startDate": 5,
                  "endDate": 6,
                  "leaveStart": {
                     $dateFromString: {
                        dateString: "$startDate",
                        format: "%d%m%Y"
                     }
                  },
                  "leaveEnd": {
                     $dateFromString: {
                        dateString: "$endDate",
                        format: "%d%m%Y"
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
                           { "leaveStart": { "$lte": leaveBegin } },
                           { "leaveEnd": { "$gte": leaveDone } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$lte": leaveBegin } },
                           { "leaveEnd": { "$gte": leaveBegin } },
                           { "leaveEnd": { "$lte": leaveDone } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$gte": leaveBegin } },
                           { "leaveStart": { "$lte": leaveDone } },
                           { "leaveEnd": { "$gte": leaveBegin } },
                           { "leaveEnd": { "$lte": leaveDone } }
                        ]
                     }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$_id",
                  "ctsEmpId": "$ctsEmpId",
                  "startDate": "$leaveStart",
                  "endDate": "$leaveEnd",
                  "leaveBegin": leaveBegin,
                  "leaveDone": leaveDone,
                  "calcDays": {
                     $switch: {
                        branches: [
                           { case: { "$eq": ["$leaveStart", "$leaveEnd"] }, then: 1 },
                           {
                              case: {
                                 $and: [
                                    { $lte: ["$leaveStart", leaveBegin] },
                                    { $gte: ["$leaveEnd", leaveDone] }
                                 ]
                              }, then: {
                                 $add: [{ $subtract: [leaveBegin, leaveDone] }, 1]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $lte: ["$leaveStart", leaveBegin] },
                                    { $gte: ["$leaveEnd", leaveBegin] },
                                    { $lte: ["$leaveEnd", leaveDone] }
                                 ]
                              }, then: {
                                 $add: [{ $subtract: [leaveBegin, "$leaveEnd"] }, 1]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $gte: ["$leaveStart", leaveBegin] },
                                    { $lte: ["$leaveStart", leaveDone] },
                                    { $gte: ["$leaveEnd", leaveBegin] },
                                    { $lte: ["$leaveEnd", leaveDone] }
                                 ]
                              }, then: {
                                 $add: [{ $subtract: ["$leaveStart", leaveDone] }, 1]
                              }
                           }
                        ]
                     }
                  }
               }
            },
            {
               $group: {
                  "_id": "$ctsEmpId",
                  "totalDays": { "$sum": "$calcDays" }
               }
            }
         ]).toArray((err, leaveDaysObj) => {
            if (err) {
               reject("DB error in " + countPersonalDays.name + ": " + err);
            } else if (leaveDaysObj.length >= 1) {
               resolve(leaveDaysObj[0].totalDays);
            } else {
               resolve(0);
            }
         });
      }
   });
}


function countLocationHolidays(cityCode, monthStartDate, monthEndDate) {
   return new Promise((resolve, reject) => {
      if (cityCode === undefined || cityCode === "") {
         reject(countLocationHolidays.name + ": City code is not provided");
      } else if (monthStartDate === undefined || monthStartDate === "") {
         reject(countLocationHolidays.name + ": Leave start date is not provided");
      } else if (monthEndDate === undefined || monthEndDate === "") {
         reject(countLocationHolidays.name + ": Leave end date is not provided");
      } else {
         let leaveBegin = new Date(monthStartDate);
         leaveBegin.setUTCHours(0, 0, 0, 0);
         let leaveDone = new Date(monthEndDate);
         leaveDone.setUTCHours(0, 0, 0, 0);
         dbObj.getDb().collection(locLeaveColl).aggregate([
            {
               $project: {
                  "_id": 1,
                  "cityCode": 2,
                  "startDate": 3,
                  "endDate": 4,
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
                  "cityCode": cityCode,
                  $or: [
                     {
                        $and: [
                           { "leaveStart": { "$lte": leaveBegin } },
                           { "leaveEnd": { "$gte": leaveDone } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$lte": leaveBegin } },
                           { "leaveEnd": { "$gte": leaveBegin } },
                           { "leaveEnd": { "$lte": leaveDone } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$gte": leaveBegin } },
                           { "leaveStart": { "$lte": leaveDone } },
                           { "leaveEnd": { "$gte": leaveBegin } },
                           { "leaveEnd": { "$lte": leaveDone } }
                        ]
                     }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$_id",
                  "cityCode": "$cityCode",
                  "startDate": "$leaveStart",
                  "endDate": "$leaveEnd",
                  "calcDays": {
                     $switch: {
                        branches: [
                           { case: { $eq: ["$leaveStart", "$leaveEnd"] }, then: 1 },
                           {
                              case: {
                                 $and: [
                                    { $lte: ["$leaveStart", leaveBegin] },
                                    { $gte: ["$leaveEnd", leaveDone] }
                                 ]
                              }, then: {
                                 $add: [{ $subtract: [leaveBegin, leaveDone] }, 1]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $lte: ["$leaveStart", leaveBegin] },
                                    { $gte: ["$leaveEnd", leaveBegin] },
                                    { $lte: ["$leaveEnd", leaveDone] }
                                 ]
                              }, then: {
                                 $add: [{ $subtract: [leaveBegin, "$leaveEnd"] }, 1]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $gte: ["$leaveStart", leaveBegin] },
                                    { $lte: ["$leaveStart", leaveDone] },
                                    { $gte: ["$leaveEnd", leaveBegin] },
                                    { $lte: ["$leaveEnd", leaveDone] }
                                 ]
                              }, then: {
                                 $add: [{ $subtract: ["$leaveStart", leaveDone] }, 1]
                              }
                           }
                        ]
                     }
                  }
               }
            },
            {
               $group: {
                  "_id": "$cityCode",
                  "totalDays": { "$sum": "$calcDays" }
               }
            }
         ]).toArray((err, leaveDaysObj) => {
            if (err) {
               reject("DB error in " + countLocationHolidays.name + ": " + err);
            } else if (leaveDaysObj.length >= 1) {
               resolve(leaveDaysObj[0].totalDays);
            } else {
               resolve(0);
            }
         });
      }
   });
}


module.exports = {
   computeWeekdaysInLeave,
   computeLeaveDays,
   getDaysBetween,
   countPersonalDays,
   countLocationHolidays
}