const dbObj = require("./database");
const empLeaveColl = "emp_leave";
const locLeaveColl = "loc_holiday";
const mSecInDay = 86400000;

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

function countPersonalDays(empEsaLink, ctsEmpId, fromDate, toDate) {
   return new Promise((resolve, reject) => {
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(getPersonalLeave.name + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(getPersonalLeave.name + ": Employee ID is not provided");
      } else if (fromDate === undefined || fromDate === "") {
         reject(getPersonalLeave.name + ": Leave start date is not provided");
      } else if (toDate === undefined || toDate === "") {
         reject(getPersonalLeave.name + ": Leave end date is not provided");
      } else {
         let startDate = new Date(fromDate);
         startDate.setHours(0, 0, 0, 0);
         let stopDate = new Date(toDate);
         stopDate.setHours(0, 0, 0, 0);
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
                           { "leaveStart": { "$lte": startDate } },
                           { "leaveEnd": { "$gte": stopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$lte": startDate } },
                           { "leaveEnd": { "$gte": startDate } },
                           { "leaveEnd": { "$lte": stopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$gte": startDate } },
                           { "leaveStart": { "$lte": stopDate } },
                           { "leaveEnd": { "$gte": startDate } },
                           { "leaveEnd": { "$lte": stopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$gte": startDate } },
                           { "leaveStart": { "$lte": stopDate } },
                           { "leaveEnd": { "$gte": startDate } },
                           { "leaveEnd": { "$gte": stopDate } }
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
                  "calcDays": {
                     $switch: {
                        branches: [
                           { case: { "$eq": ["$leaveStart", "$leaveEnd"] }, then: 1 },
                           {
                              case: {
                                 $and: [
                                    { $lte: ["$leaveStart", startDate] },
                                    { $gte: ["$leaveEnd", stopDate] }
                                 ]
                              }, then: {
                                 $divide: [{ $subtract: [stopDate, startDate] }, mSecInDay]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $lte: ["$leaveStart", startDate] },
                                    { $gte: ["$leaveEnd", startDate] },
                                    { $lte: ["$leaveEnd", stopDate] }
                                 ]
                              }, then: {
                                 $divide: [{ $subtract: ["$leaveEnd", startDate] }, mSecInDay]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $gte: ["$leaveStart", startDate] },
                                    { $lte: ["$leaveStart", stopDate] },
                                    { $gte: ["$leaveEnd", startDate] },
                                    { $lte: ["$leaveEnd", stopDate] }
                                 ]
                              }, then: {
                                 $divide: [{ $subtract: ["$leaveEnd", "$leaveStart"] }, mSecInDay]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $gte: ["$leaveStart", startDate] },
                                    { $lte: ["$leaveStart", stopDate] },
                                    { $gte: ["$leaveEnd", startDate] },
                                    { $gte: ["$leaveEnd", stopDate] }
                                 ]
                              }, then: {
                                 $divide: [{ $subtract: [stopDate, "$leaveStart"] }, mSecInDay]
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


function countLocationHolidays(cityCode, fromDate, toDate) {
   return new Promise((resolve, reject) => {
      if (cityCode === undefined || cityCode === "") {
         reject(countLocationHolidays.name + ": City code is not provided");
      } else if (fromDate === undefined || fromDate === "") {
         reject(countLocationHolidays.name + ": Leave start date is not provided");
      } else if (toDate === undefined || toDate === "") {
         reject(countLocationHolidays.name + ": Leave end date is not provided");
      } else {
         let startDate = new Date(fromDate);
         startDate.setUTCHours(0, 0, 0, 0);
         let stopDate = new Date(toDate);
         stopDate.setUTCHours(0, 0, 0, 0);
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
                           { "leaveStart": { "$lte": startDate } },
                           { "leaveEnd": { "$gte": stopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$lte": startDate } },
                           { "leaveEnd": { "$gte": startDate } },
                           { "leaveEnd": { "$lte": stopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$gte": startDate } },
                           { "leaveStart": { "$lte": stopDate } },
                           { "leaveEnd": { "$gte": startDate } },
                           { "leaveEnd": { "$lte": stopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$gte": startDate } },
                           { "leaveStart": { "$lte": stopDate } },
                           { "leaveEnd": { "$gte": startDate } },
                           { "leaveEnd": { "$gte": stopDate } }
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
                                    { $lte: ["$leaveStart", startDate] },
                                    { $gte: ["$leaveEnd", stopDate] }
                                 ]
                              }, then: {
                                 $divide: [{ $subtract: [stopDate, startDate] }, mSecInDay]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $lte: ["$leaveStart", startDate] },
                                    { $gte: ["$leaveEnd", startDate] },
                                    { $lte: ["$leaveEnd", stopDate] }
                                 ]
                              }, then: {
                                 $divide: [{ $subtract: ["$leaveEnd", startDate] }, mSecInDay]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $gte: ["$leaveStart", startDate] },
                                    { $lte: ["$leaveStart", stopDate] },
                                    { $gte: ["$leaveEnd", startDate] },
                                    { $lte: ["$leaveEnd", stopDate] }
                                 ]
                              }, then: {
                                 $divide: [{ $subtract: ["$leaveEnd", "$leaveStart"] }, mSecInDay]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $gte: ["$leaveStart", startDate] },
                                    { $lte: ["$leaveStart", stopDate] },
                                    { $gte: ["$leaveEnd", startDate] },
                                    { $gte: ["$leaveEnd", stopDate] }
                                 ]
                              }, then: {
                                 $divide: [{ $subtract: [stopDate, "$leaveStart"] }, mSecInDay]
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