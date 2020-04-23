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
function getDaysBetween(startDate, stopDate, getWeekDays) {
   let daysBetween = 0;
   return new Promise((resolve, _reject) => {
      if (startDate === undefined || stopDate === undefined) {
         resolve(daysBetween);
      } else {
         /* clone date to avoid messing up original data */
         let fromDate = new Date(startDate);
         let toDate = new Date(stopDate);

         fromDate.setUTCHours(0, 0, 0, 0);
         toDate.setUTCHours(0, 0, 0, 0);

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

function countPersonalWeekdays(empEsaLink, ctsEmpId, cityCode, selfLeaveStart, selfLeaveStop) {
   return new Promise((resolve, reject) => {
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(countPersonalWeekdays.name + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(countPersonalWeekdays.name + ": Employee ID is not provided");
      } else if (selfLeaveStart === undefined || selfLeaveStart === "") {
         reject(countPersonalWeekdays.name + ": Leave start date is not provided");
      } else if (selfLeaveStop === undefined || selfLeaveStop === "") {
         reject(countPersonalWeekdays.name + ": Leave stop date is not provided");
      } else if (cityCode === undefined || cityCode === "") {
         reject(countPersonalWeekdays.name + ": City code is not provided");
      } else {
         let refStartDate = new Date(selfLeaveStart);
         refStartDate.setUTCHours(0, 0, 0, 0);
         let refStopDate = new Date(selfLeaveStop);
         refStopDate.setUTCHours(23, 59, 59, 0);
         console.log(countPersonalWeekdays.name + " - refStartDate: " + refStartDate);
         console.log(countPersonalWeekdays.name + " - refStopDate: " + refStopDate);
         dbObj.getDb().collection(empLeaveColl).aggregate([
            {
               $project: {
                  "_id": 1,
                  "empEsaLink": 3,
                  "ctsEmpId": 4,
                  "startDate": 5,
                  "endDate": 6,
                  "leaveStart": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$startDate", 4, -1] } },
                        month: { $toInt: { $substr: ["$startDate", 2, 2] } },
                        day: { $toInt: { $substr: ["$startDate", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                     }
                  },
                  "leaveStop": {
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
                  $or: [
                     {
                        $and: [
                           { "leaveStart": { "$lte": refStartDate } },
                           { "leaveStop": { "$gte": refStopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$lte": refStartDate } },
                           { "leaveStop": { "$gte": refStartDate } },
                           { "leaveStop": { "$lte": refStopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$gte": refStartDate } },
                           { "leaveStart": { "$lte": refStopDate } },
                           { "leaveStop": { "$gte": refStartDate } },
                           { "leaveStop": { "$lte": refStopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$gte": refStartDate } },
                           { "leaveStart": { "$lte": refStopDate } },
                           { "leaveStop": { "$gte": refStartDate } },
                           { "leaveStop": { "$lte": refStopDate } }
                        ]
                     }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$_id",
                  "startDate": "$leaveStart",
                  "endDate": "$leaveStop"
               }
            }
         ]).toArray((err, leaveArr) => {
            if (err) {
               reject("DB error in " + countPersonalWeekdays.name + ": " + err);
            } else if (leaveArr.length >= 1) {
               computeWeekdaysInLeave(leaveArr).then((workDaysInLeave) => {
                  /*console.log("weekdays between [" + leaveArr[0].startDate + "] and [" + leaveArr[0].endDate + "] ===> " + workDaysInLeave);*/
                  ovrlpngLocHolidays(leaveArr, cityCode).then((ovrlpngLocDays) => {
                     resolve(workDaysInLeave - ovrlpngLocDays);
                  });
               });
            }
            else {
               resolve(0);
            }
         });
      }
   });
}


function ovrlpngLocHolidays(leaveArr, cityCode) {
   let ovrlpngLocDays = 0;
   return new Promise(async (resolve, reject) => {
      await leaveArr.forEach((leave) => {
         let leaveStart = new Date(leave.startDate);
         leaveStart.setUTCHours(0, 0, 0, 0);
         let leaveStop = new Date(leave.endDate);
         leaveStop.setUTCHours(23, 59, 59, 0);
         console.log(ovrlpngLocHolidays.name + " - leaveStart: " + leaveStart);
         console.log(ovrlpngLocHolidays.name + " - leaveStop: " + leaveStop);

         countLocationWeekdays(cityCode, leaveStart, leaveStop).then((locWeekDays) => {
            ovrlpngLocDays += locWeekDays;
         });
      });
      console.log(leaveArr);
      console.log(ovrlpngLocDays);
      console.log();
      resolve(ovrlpngLocDays);
   });
}


function countLocationWeekdays(cityCode, locLeaveStart, locLeaveStop) {
   return new Promise((resolve, reject) => {
      if (cityCode === undefined || cityCode === "") {
         reject(countLocationHolidays.name + ": City code is not provided");
      } else if (locLeaveStart === undefined || locLeaveStart === "") {
         reject(countLocationHolidays.name + ": Leave start date is not provided");
      } else if (locLeaveStop === undefined || locLeaveStop === "") {
         reject(countLocationHolidays.name + ": Leave end date is not provided");
      } else {
         let refStartDate = new Date(locLeaveStart);
         refStartDate.setHours(0, 0, 0, 0);
         let refStopDate = new Date(locLeaveStop);
         refStopDate.setHours(23, 59, 59, 0);
         console.log(countPersonalWeekdays.name + " - refStartDate: " + refStartDate);
         console.log(countPersonalWeekdays.name + " - refStopDate: " + refStopDate);
         console
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
                  "leaveStop": {
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
                           { "leaveStart": { "$lte": refStartDate } },
                           { "leaveStop": { "$gte": refStopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$lte": refStartDate } },
                           { "leaveStop": { "$gte": refStartDate } },
                           { "leaveStop": { "$lte": refStopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$gte": refStartDate } },
                           { "leaveStart": { "$lte": refStopDate } },
                           { "leaveStop": { "$gte": refStartDate } },
                           { "leaveStop": { "$lte": refStopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$gte": refStartDate } },
                           { "leaveStart": { "$lte": refStopDate } },
                           { "leaveStop": { "$gte": refStartDate } },
                           { "leaveStop": { "$lte": refStopDate } }
                        ]
                     }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$_id",
                  "startDate": "$leaveStart",
                  "endDate": "$leaveStop"
               }
            }
         ]).toArray((err, leaveArr) => {
            if (err) {
               reject("DB error in " + countLocationWeekdays.name + ": " + err);
            } else if (leaveArr.length >= 1) {
               computeWeekdaysInLeave(leaveArr).then((workDaysInLeave) => {
                  resolve(workDaysInLeave);
               });
            } else {
               resolve(0);
            }
         });
      }
   });
}



function countPersonalWeekdays_test(empEsaLink, ctsEmpId, cityCode, selfLeaveStart, selfLeaveStop) {
   return new Promise((resolve, reject) => {
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(countPersonalWeekdays_test.name + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(countPersonalWeekdays_test.name + ": Employee ID is not provided");
      } else if (selfLeaveStart === undefined || selfLeaveStart === "") {
         reject(countPersonalWeekdays_test.name + ": Leave start date is not provided");
      } else if (selfLeaveStop === undefined || selfLeaveStop === "") {
         reject(countPersonalWeekdays_test.name + ": Leave stop date is not provided");
      } else if (cityCode === undefined || cityCode === "") {
         reject(countPersonalWeekdays_test.name + ": City code is not provided");
      }
      console.log(selfLeaveStart);
      console.log(selfLeaveStop);
      dbObj.getDb().collection(empLeaveColl).aggregate([
         {
            $project: {
               "_id": "$_id",
               "empEsaLink": "$empEsaLink",
               "ctsEmpId": "$ctsEmpId",
               "startDate": "$startDate",
               "endDate": "$endDate",
               "leaveStart": {
                  $dateFromParts: {
                     year: { $toInt: { $substr: ["$startDate", 4, -1] } },
                     month: { $toInt: { $substr: ["$startDate", 2, 2] } },
                     day: { $toInt: { $substr: ["$startDate", 0, 2] } },
                     hour: 0, minute: 0, second: 0, millisecond: 0
                  }
               },
               "leaveStop": {
                  $dateFromParts: {
                     year: { $toInt: { $substr: ["$endDate", 4, -1] } },
                     month: { $toInt: { $substr: ["$endDate", 2, 2] } },
                     day: { $toInt: { $substr: ["$endDate", 0, 2] } },
                     hour: 0, minute: 0, second: 0, millisecond: 0
                  }
               },
               "inputStart": {
                  $dateFromString: {
                     dateString: selfLeaveStart
                  }
               },
               "inputStop": {
                  $dateFromString: {
                     dateString: selfLeaveStop
                  }
               }
            }
         },
         {
            $match: {
               "empEsaLink": empEsaLink,
               "ctsEmpId": ctsEmpId,
               $expr: {
                  $or: [
                     {
                        $and: [
                           { $lte: ["$leaveStart", "$inputStart"] },
                           { $gte: ["$leaveStop", "$inputStop"] }
                        ]
                     },
                     {
                        $and: [
                           { $lte: ["$leaveStart", "$inputStart"] },
                           { $gte: ["$leaveStop", "$inputStart"] },
                           { $lte: ["$leaveStop", "$inputStop"] }
                        ]
                     },
                     {
                        $and: [
                           { $gte: ["$leaveStart", "$inputStart"] },
                           { $lte: ["$leaveStart", "$inputStop"] },
                           { $gte: ["$leaveStop", "$inputStart"] },
                           { $lte: ["$leaveStop", "$inputStop"] }
                        ]
                     },
                     {
                        $and: [
                           { $gte: ["$leaveStart", "$inputStart"] },
                           { $lte: ["$leaveStart", "$inputStop"] },
                           { $gte: ["$leaveStop", "$inputStart"] },
                           { $gte: ["$leaveStop", "$inputStop"] }
                        ]
                     }
                  ]
               }
            }
         },
         {
            $project: {
               "_id": "$_id",
               "empEsaLink": "$empEsaLink",
               "ctsEmpId": "$ctsEmpId",
               "startDate": "$startDate",
               "endDate": "$endDate",
               "leaveStart": "$leaveStart",
               "leaveStop": "$leaveStop",
               "inputStart": "$inputStart",
               "inputStop": "$inputStop"
            }
         },
      ]).toArray((err, leaveArr) => {
         if (err) {
            reject(err);
         } else {
            resolve(leaveArr);
         }
      });
   });
}



module.exports = {
   computeWeekdaysInLeave,
   computeLeaveDays,
   getDaysBetween,
   countPersonalWeekdays,
   countLocationWeekdays,
   countPersonalWeekdays_test
}