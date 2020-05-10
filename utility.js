const dbObj = require("./database");
const empObj = require("./employee");
const locObj = require("./location");
const empLeaveColl = "emp_leave";
const leaveHour = 4;

function countWeekdays(leaveArr, callerName) {
   let funcName = countWeekdays.name;
   let weekdaysInLeave = 0;
   return new Promise(async (resolve, _reject) => {
      await leaveArr.forEach((leave) => {
         getWeekDaysBetween(leave.startDate, leave.endDate, true, funcName).then((weekdays) => {
            weekdaysInLeave += weekdays;
         });
      });
      resolve(weekdaysInLeave);
   });
}

function countWeekends(leaveArr, callerName) {
   let funcName = countWeekends.name;
   let weekendsInLeave = 0;
   return new Promise(async (resolve, _reject) => {
      await leaveArr.forEach((leave) => {
         getWeekEndsBetween(leave.startDate, leave.endDate, true, funcName).then((weekends) => {
            weekendsInLeave += weekends;
         });
      });
      resolve(weekendsInLeave);
   });
}

function countAllDays(leaveArr, callerName) {
   let funcName = countAllDays.name;
   let leaveDays = 0;
   return new Promise(async (resolve, _reject) => {
      await leaveArr.forEach((leave) => {
         getWeekDaysBetween(leave.startDate, leave.endDate, false, funcName).then((daysBetween) => {
            leaveDays += daysBetween;
         });
      });
      resolve(leaveDays);
   });
}

function getWeekEndsBetween(startDate, stopDate, callerName) {
   let funcName = getWeekEndsBetween.name;
   let daysBetween = 0;
   return new Promise((resolve, _reject) => {
      if (startDate === undefined || stopDate === undefined) {
         resolve(daysBetween);
      } else {
         /* clone date to avoid messing up original data */
         let fromDate = new Date(startDate);
         let toDate = new Date(stopDate);

         fromDate.setHours(0, 0, 0, 0);
         toDate.setHours(0, 0, 0, 0);

         if (fromDate.getTime() === toDate.getTime()) {
            let dayOfWeek = fromDate.getDay();
            if (dayOfWeek === 0 || dayOfWeek === 6) {
               daysBetween++;
            }
         } else {
            while (fromDate <= toDate) {
               let dayOfWeek = fromDate.getDay();
               /* check if the date is neither a Sunday(0) nor a Saturday(6) */
               if (dayOfWeek === 0 && dayOfWeek === 6) {
                  daysBetween++;
               }
               fromDate.setDate(fromDate.getDate() + 1);
            }
         }
         resolve(daysBetween);
      }
   });
};

/* calculate number of days between */
function getWeekDaysBetween(startDate, stopDate, getWeekDays, callerName) {
   let funcName = getWeekDaysBetween.name;
   let daysBetween = 0;
   return new Promise((resolve, _reject) => {
      if (startDate === undefined || stopDate === undefined) {
         resolve(daysBetween);
      } else {
         /* clone date to avoid messing up original data */
         let fromDate = new Date(startDate);
         let toDate = new Date(stopDate);

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


function calcLeaveHours(wrkHrPerDay, selfLeaveArr, locHolArr, leaveStart, leaveStop, callerName) {
   let funcName = calcLeaveHours.name;
   let totalWorkHours = 0;
   return new Promise(async (resolve, reject) => {
      let refStartDate = new Date(leaveStart);
      refStartDate.setUTCHours(0, 0, 0, 0);
      let refStopDate = new Date(leaveStop);
      refStopDate.setUTCHours(0, 0, 0, 0);
      await selfLeaveArr.forEach(async (selfLeave) => {
         let selfLeaveStart = new Date(selfLeave.startDate);
         selfLeaveStart.setUTCHours(0, 0, 0, 0);
         let selfLeaveStop = new Date(selfLeave.stopDate);
         selfLeaveStop.setUTCHours(0, 0, 0, 0);

         let calcStart = new Date(selfLeaveStart);
         let calcStop = new Date(selfLeaveStop);
         if (selfLeaveStart.getTime() < refStartDate.getTime()) {
            calcStart = refStartDate;
         }
         if (selfLeaveStop.getTime() > refStopDate.getTime()) {
            calcStop = refStopDate;
         }
         calcStart.setUTCHours(0, 0, 0, 0);
         calcStop.setUTCHours(0, 0, 0, 0);

         getWeekDaysBetween(calcStart, calcStop, true, funcName).then(async (workDaysBetween) => {
            if (selfLeave.halfDayInd === 1) {
               totalWorkHours += (wrkHrPerDay - leaveHour) * workDaysBetween;
            }
            await locHolArr.forEach((locHol) => {
               let locHolStart = new Date(locHol.startDate);
               locHolStart.setUTCHours(0, 0, 0, 0);
               let locHolStop = new Date(locHol.stopDate);
               locHolStop.setUTCHours(0, 0, 0, 0);

               if (selfLeaveStart.getTime() >= locHolStart.getTime()) {
                  effLeaveStart = selfLeaveStart
               } else if (locHolStart.getTime() >= selfLeaveStart.getTime()) {
                  effLeaveStart = locHolStart;
               }

               if (selfLeaveStop.getTime() >= locHolStop.getTime()) {
                  effLeaveStop = selfLeaveStop;
               } else if (locHolStop.getTime() >= selfLeaveStop.getTime()) {
                  effLeaveStop = locHolStop;
               }

               effLeaveStart.setUTCHours(0, 0, 0, 0);
               effLeaveStop.setUTCHours(0, 0, 0, 0);

               if (refStartDate > effLeaveStart) {
                  effLeaveStart = refStartDate;
               }

               if (effLeaveStop < refStopDate) {
                  effLeaveStop = refStopDate;
               }

               effLeaveStart.setUTCHours(0, 0, 0, 0);
               effLeaveStop.setUTCHours(0, 0, 0, 0);

               if (selfLeave.halfDayInd === 1 && locHol.halfDayInd === 1) {
                  totalWorkHours += (wrkHrPerDay / 2);
               } else {
                  totalWorkHours += wrkHrPerDay;
               }
            });
         });
      });
      resolve(totalWorkHours);
   });
}

/*
function calcSelfAndLocLeaveHours(empEsaLink, ctsEmpId, wrkHrPerDay, cityCode, leaveStart, leaveStop, callerName) {
   let leaveHours = 0;
   let funcName = calcSelfAndLocLeaveHours.name;
   return new Promise((resolve, reject) => {
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(getEffectiveLeaveDays.name + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(getEffectiveLeaveDays.name + ": Employee ID is not provided");
      } else if (wrkHrPerDay === undefined || wrkHrPerDay === "") {
         reject(getEffectiveLeaveDays.name + ": Billing hour per day is not provided");
      } else if (cityCode === undefined || cityCode === "") {
         reject(getEffectiveLeaveDays.name + ": Work city code is not provided");
      } else if (leaveStart === undefined || leaveStart === "") {
         reject(getEffectiveLeaveDays.name + ": Leave start date is not provided");
      } else if (leaveStop === undefined || leaveStop) {
         reject(getEffectiveLeaveDays.name + ": Leave stop date is not provided");
      } else {
         empObj.getSelfLeaveDates(empEsaLink, ctsEmpId, leaveStart, leaveStop, funcName).then((selfLeaveArr) => {
            countWeekdays(selfLeaveArr).then((selfLeaveWorkdays) => {
               leaveHours = (selfLeaveWorkdays + splWrkWeekends) * wrkHrPerDay;

               locObj.getLocHolDates(cityCode, leaveStart, leaveStop, funcName).then((locHolArr) => {
                  splWrkObj.getLocSplWrkDates(cityCode, leaveStart, leaveStop, funcName).then((locSplWrkArr) => {
                     resolve(locHolArr)
                  });
               });
            });
         });
      }
   });
}
*/

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
      } else {
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
      }
   });
}



module.exports = {
   countWeekdays,
   countAllDays,
   countWeekends,
   calcLeaveHours,
   getWeekDaysBetween,
   getWeekEndsBetween,
   /*calcSelfAndLocLeaveHours,*/
   countPersonalWeekdays_test
}