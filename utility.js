const dbObj = require("./database");
const esaProjColl = "esa_proj";
const leaveHour = 4;

/* get list of projects */
function getProjectList(callerName) {
   return new Promise((resolve, reject) => {
      dbObj.getDb().collection(esaProjColl).aggregate([
         {
            $group: {
               "_id": "$esaId",
               "currency": { $first: "$currency" },
               "billingMode": { $first: "$billingMode" },
               "description": {
                  $addToSet: {
                     "name": "$esaDesc",
                     "subType": "$esaSubType"
                  }
               }
            }
         },
         { $unwind: "$description" },
         { $sort: { "description.subType": 1 } },
         { $group: { "_id": "$_id", "description": { $push: "$description" } } },
         { $unwind: "$_id" },
         { $sort: { "_id": 1 } }
      ]).toArray(function (err, projectList) {
         if (err) {
            reject(err);
         } else {
            resolve(projectList);
         }
      });
   });
}

function countWeekdays(leaveArr, callerName) {
   let funcName = countWeekdays.name;
   let weekdaysInLeave = 0;
   return new Promise(async (resolve, reject) => {
      await leaveArr.forEach((leave) => {
         getDaysBetween(leave.startDate, leave.stopDate, true, false, funcName).then((weekdays) => {
            if (leave.halfDay === "Y") {
               weekdaysInLeave += (weekdays / 2);
            } else {
               weekdaysInLeave += weekdays;
            }
         });
      });
      resolve(weekdaysInLeave);
   });
}

function countWeekends(leaveArr, callerName) {
   let funcName = countWeekends.name;
   let weekendsInLeave = 0;
   return new Promise(async (resolve, reject) => {
      await leaveArr.forEach((leave) => {
         getDaysBetween(leave.startDate, leave.stopDate, false, true, funcName).then((weekends) => {
            if (leave.halfDay === "Y") {
               weekendsInLeave += (weekends / 2);
            } else {
               weekendsInLeave += weekends;
            }
         });
      });
      resolve(weekendsInLeave);
   });
}

function countAllDays(leaveArr, callerName) {
   let funcName = countAllDays.name;
   let leaveDays = 0;
   return new Promise(async (resolve, reject) => {
      await leaveArr.forEach((leave) => {
         getDaysBetween(leave.startDate, leave.stopDate, true, true, funcName).then((daysBetween) => {
            if (leave.halfDay === "Y") {
               leaveDays += (daysBetween / 2);
            } else {
               leaveDays += daysBetween;
            }
         });
      });
      resolve(leaveDays);
   });
}

/* calculate number of days between */
function getDaysBetween(startDate, stopDate, inclWeekdays, inclWeekends, callerName) {
   let funcName = getDaysBetween.name;
   let daysBetween = 0;
   return new Promise((resolve, reject) => {
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
            if ((inclWeekdays === true && dayOfWeek > 0 && dayOfWeek < 6) ||
               (inclWeekends === true && (dayOfWeek === 0 || dayOfWeek === 6))) {
               daysBetween++;
            }
         } else {
            while (fromDate <= toDate) {
               let dayOfWeek = fromDate.getDay();
               /* check if the date is neither a Sunday(0) nor a Saturday(6) */
               if ((inclWeekdays === true && dayOfWeek > 0 && dayOfWeek < 6) ||
                  (inclWeekends === true && (dayOfWeek === 0 || dayOfWeek === 6))) {
                  daysBetween++;
               }
               fromDate.setDate(fromDate.getDate() + 1);
            }
         }
         resolve(daysBetween);
      }
   });
}


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

         getDaysBetween(calcStart, calcStop, true, funcName).then(async (workDaysBetween) => {
            if (selfLeave.halfDay === "Y") {
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

               if (selfLeave.halfDay === "Y" && locHol.halfDay === "Y") {
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


module.exports = {
   countWeekdays,
   countAllDays,
   countWeekends,
   calcLeaveHours,
   getDaysBetween,
   getProjectList
}