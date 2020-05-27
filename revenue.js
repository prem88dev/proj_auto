const dateTime = require("date-and-time");
const dateFormat = require("dateformat");


function getBufferHours(empBufferArr, monthIndex, wrkHrPerDay, callerName) {
   let funcName = getBufferHours.name;
   let bufferHours = 0;
   return new Promise(async (resolve, reject) => {
      if (empBufferArr === undefined || empBufferArr === "") {
         reject(funcName + ": Buffer array is not defined");
      } else if (monthIndex === undefined || monthIndex === "") {
         reject(funcName + ": Month index is not provided");
      } else if (wrkHrPerDay === undefined || wrkHrPerDay === "") {
         reject(funcName + ": Billing hours per day is not provided");
      } else {
         await empBufferArr.forEach((empBuffer, idx) => {
            if (idx < (empBufferArr.length - 1)) {
               let bufferMonth = new Date(dateTime.parse(empBuffer.month, "MMM-YYYY", true)).getMonth();
               if (monthIndex === bufferMonth) {
                  bufferHours = parseInt(empBuffer.days, 10) * parseInt(wrkHrPerDay, 10);
               }
            }
         });
         resolve(bufferHours);
      }
   });
}


function computeLeaveHour(leaveArr, leaveDate, wrkHrPerDay, callerName) {
   let funcName = computeLeaveHour.name;
   let leaveHour = 0;
   return new Promise(async (resolve, _reject) => {
      if ((leaveArr.length - 1) > 0) {
         await leaveArr.forEach((leave, idx) => {
            if (idx < (leaveArr.length - 1)) {
               let checkDate = new Date(leaveDate);
               checkDate.setHours(0, 0, 0, 0);
               let leaveStart = new Date(leave.startDate);
               leaveStart.setHours(0, 0, 0, 0);
               let leaveStop = new Date(leave.stopDate);
               leaveStop.setHours(0, 0, 0, 0);
               if (checkDate.getTime() >= leaveStart.getTime() && checkDate.getTime() <= leaveStop.getTime()) {
                  if (leave.halfDay === "Y") {
                     if (leave.leaveHour !== undefined && leave.leaveHour !== "") {
                        leaveHour = leave.leaveHour;
                     } else {
                        leaveHour = 4;
                     }
                  } else {
                     leaveHour = wrkHrPerDay;
                  }
               }
            }
         });
         resolve(leaveHour);
      } else {
         resolve(leaveHour);
      }
   });
}


function calcRevenueHour(empProjection, startDate, stopDate, revenueHourArr, callerName) {
   let funcName = calcRevenueHour.name;
   let wrkHrPerDay = 0;
   if (empProjection[0].wrkHrPerDay !== undefined && empProjection[0].wrkHrPerDay !== "") {
      wrkHrPerDay = parseInt(empProjection[0].wrkHrPerDay, 10);
   }
   return new Promise(async (resolve, _reject) => {
      let revenueHour = 0;
      let cmiRevenueHour = 0;
      let calcStartDate = new Date(startDate);
      calcStartDate.setHours(0, 0, 0, 0);
      let calcStopDate = new Date(stopDate);
      calcStopDate.setHours(0, 0, 0, 0);
      let tmpHourArr = { "revenueHour": revenueHour, "cmiRevenueHour": cmiRevenueHour, "nextStartDate": stopDate };

      if (calcStartDate.getTime() <= calcStopDate.getTime()) {
         if (revenueHourArr !== undefined && revenueHourArr !== "") {
            if (revenueHourArr.revenueHour !== undefined && revenueHourArr.revenueHour !== "") {
               revenueHour = parseInt(revenueHourArr.revenueHour, 10);
            }
            if (revenueHourArr.cmiRevenueHour !== undefined && revenueHourArr.cmiRevenueHour !== "") {
               cmiRevenueHour = parseInt(revenueHourArr.cmiRevenueHour, 10);
            }
            if (revenueHourArr.nextStartDate !== undefined && revenueHourArr.nextStartDate !== "") {
               calcStartDate = new Date(revenueHourArr.nextStartDate);
               calcStartDate.setHours(0, 0, 0, 0);
            }
         }

         let dayOfWeek = calcStartDate.getDay();
         let intSelfLeaveHour = 0;
         let intLocLeaveHour = 0;
         let intSelfAddlHour = 0;
         let intLocAddlHour = 0

         await computeLeaveHour(empProjection[1].leaves, calcStartDate, wrkHrPerDay, funcName).then((selfLeaveHour) => {
            intSelfLeaveHour = parseInt(selfLeaveHour, 10);
         });

         await computeLeaveHour(empProjection[2].publicHolidays, calcStartDate, wrkHrPerDay, funcName).then((locLeaveHour) => {
            intLocLeaveHour = parseInt(locLeaveHour, 10);
         });

         await computeLeaveHour(empProjection[3].specialWorkDays.empSplWrk, calcStartDate, wrkHrPerDay, funcName).then((selfAddlHour) => {
            intSelfAddlHour = parseInt(selfAddlHour, 10);
         });

         await computeLeaveHour(empProjection[3].specialWorkDays.locSplWrk, calcStartDate, wrkHrPerDay, funcName).then((locAddlHour) => {
            intLocAddlHour = parseInt(locAddlHour, 10);
         });

         if (dayOfWeek > 0 && dayOfWeek < 6) { /* weekday */
            if (intSelfLeaveHour === 0 && intLocLeaveHour === 0) { /* it's neither a personal leave nor a public holiday */
               revenueHour += wrkHrPerDay;
               cmiRevenueHour += wrkHrPerDay;
            } else if (intSelfLeaveHour > 0 && intLocLeaveHour === 0) { /* it's a personal leave and not a public holiday */
               if (intSelfAddlHour >= wrkHrPerDay) {
                  revenueHour += wrkHrPerDay;
               } else if (intSelfAddlHour > intSelfLeaveHour) {
                  revenueHour += intSelfAddlHour;
               } else if (intSelfAddlHour > 0 && intSelfAddlHour < intSelfLeaveHour) {
                  revenueHour += intSelfLeaveHour - intSelfAddlHour;
               } else if (intSelfLeaveHour === intSelfAddlHour) {
                  revenueHour += wrkHrPerDay;
               } else if (intSelfLeaveHour <= wrkHrPerDay) {
                  revenueHour += wrkHrPerDay - intSelfLeaveHour;
               }
               cmiRevenueHour += wrkHrPerDay;
            } else if (intSelfLeaveHour === 0 && intLocLeaveHour > 0) { /* it's a public holiday and there is no personal leave that overlaps it */
               /* employee might have worked on this pubic holiday */
               if (intSelfAddlHour >= wrkHrPerDay) {
                  revenueHour += wrkHrPerDay;
               } else if (intSelfAddlHour > 0) {
                  revenueHour += intSelfAddlHour;
               }

               /* public holiday might have turned to be a working day. If yes, consider those hours in revenue calculation */
               if (intLocAddlHour >= wrkHrPerDay) {
                  cmiRevenueHour += wrkHrPerDay;
               } else if (intLocAddlHour > intLocLeaveHour) {
                  cmiRevenueHour += intLocAddlHour;
               } else if (intLocAddlHour > 0 && intLocAddlHour < intLocLeaveHour) {
                  cmiRevenueHour += intLocLeaveHour - intLocAddlHour;
               } else if (intLocLeaveHour === intLocAddlHour) {
                  cmiRevenueHour += wrkHrPerDay;
               } else if (intLocLeaveHour <= wrkHrPerDay) {
                  cmiRevenueHour += wrkHrPerDay - intLocLeaveHour;
               }
            } else if (intSelfLeaveHour > 0 && intLocLeaveHour > 0) {
               if (intSelfAddlHour >= wrkHrPerDay) {
                  revenueHour += wrkHrPerDay;
               } else if (intSelfAddlHour > intSelfLeaveHour) {
                  revenueHour += intSelfAddlHour;
               } else if (intSelfAddlHour > 0 && intSelfAddlHour < intSelfLeaveHour) {
                  revenueHour += intSelfLeaveHour - intSelfAddlHour;
               } else if (intSelfLeaveHour === intSelfAddlHour) {
                  revenueHour += wrkHrPerDay;
               } else if (intSelfLeaveHour <= wrkHrPerDay) {
                  revenueHour += wrkHrPerDay - intSelfLeaveHour;
               }

               if (intLocAddlHour >= wrkHrPerDay) {
                  cmiRevenueHour += wrkHrPerDay;
               } else if (intLocAddlHour > intLocLeaveHour) {
                  cmiRevenueHour += intLocAddlHour;
               } else if (intLocAddlHour > 0 && intLocAddlHour < intLocLeaveHour) {
                  cmiRevenueHour += intLocLeaveHour - intLocAddlHour;
               } else if (intLocLeaveHour === intLocAddlHour) {
                  cmiRevenueHour += wrkHrPerDay;
               } else if (intLocLeaveHour <= wrkHrPerDay) {
                  cmiRevenueHour += wrkHrPerDay - intLocLeaveHour;
               }
            }
         } else { /* weekend */
            if (intSelfAddlHour >= wrkHrPerDay) {
               revenueHour += wrkHrPerDay;
            } else if (intSelfAddlHour > 0) {
               revenueHour += intSelfAddlHour;
            }

            if (intLocAddlHour >= wrkHrPerDay) {
               cmiRevenueHour += wrkHrPerDay;
            } else if (intLocAddlHour > 0) {
               cmiRevenueHour += intLocAddlHour;
            }
         }

         calcStartDate.setDate(calcStartDate.getDate() + 1);
         tmpHourArr = { "revenueHour": revenueHour, "cmiRevenueHour": cmiRevenueHour, "nextStartDate": calcStartDate };

         /* if (calcStartDate.getMonth() === 0 || (calcStartDate.getMonth() === 1 && calcStartDate.getDate() === 1)) {
            console.log({ "revenueHour": revenueHour, "cmiRevenueHour": cmiRevenueHour, "nextStartDate": dateFormat(calcStartDate, "d-mmm-yyyy", false) });
         } */

         if (calcStartDate.getTime() > calcStopDate.getTime()) {
            return resolve(tmpHourArr);
         } else {
            return resolve(calcRevenueHour(empProjection, calcStartDate, stopDate, tmpHourArr, funcName));
         }
      }
   });
}


function getMonthlyRevenue(empProjection, revenueYear, monthIndex, callerName) {
   let funcName = getMonthlyRevenue.name;
   return new Promise(async (resolve, _reject) => {
      let wrkHrPerDay = parseInt(empProjection[0].wrkHrPerDay, 10);
      let billRatePerHr = parseInt(empProjection[0].billRatePerHr, 10);
      let calcYear = parseInt(revenueYear, 10);
      let calcStartMonth = parseInt(monthIndex, 10);
      let calcStopMonth = parseInt(monthIndex, 10) + 1;

      let monthFirstDate = new Date(calcYear, calcStartMonth, 1);
      monthFirstDate.setHours(0, 0, 0, 0);
      let monthLastDate = new Date(calcYear, calcStopMonth, 0);
      monthLastDate.setHours(0, 0, 0, 0);

      let sowStart = new Date(dateTime.parse(empProjection[0].sowStart, "D-MMM-YYYY", true));
      sowStart.setHours(0, 0, 0, 0);
      let sowEnd = new Date(dateTime.parse(empProjection[0].sowStop, "D-MMM-YYYY", true));
      sowEnd.setHours(0, 0, 0, 0);

      let effStopDate = sowEnd;
      if (empProjection[0].foreseenSowStop !== undefined && empProjection[0].foreseenSowStop !== "") {
         let foreseenStopDate = new Date(dateTime.parse(empProjection[0].foreseenSowStop, "D-MMM-YYYY", true));
         foreseenStopDate.setHours(0, 0, 0, 0);

         if ((foreseenStopDate.getFullYear() === sowEnd.getFullYear()) && (foreseenStopDate.getMonth() > sowEnd.getMonth())) {
            effStopDate = foreseenStopDate;
         }
      }

      let calcStartDate = new Date(monthFirstDate);
      calcStartDate.setHours(0, 0, 0, 0);
      let calcStopDate = new Date(monthLastDate);
      calcStopDate.setHours(0, 0, 0, 0);

      if ((sowStart.getFullYear() === calcStartDate.getFullYear()) && (sowStart.getMonth() === calcStartDate.getMonth())) {
         calcStartDate = sowStart;
      }

      if ((effStopDate.getFullYear() === calcStopDate.getFullYear()) && (effStopDate.getMonth() === calcStopDate.getMonth())) {
         calcStopDate = effStopDate;
      }

      let intBufferHour = 0;
      await getBufferHours(empProjection[4].buffers, monthIndex, wrkHrPerDay, funcName).then((bufferHours) => {
         intBufferHour = parseInt(bufferHours, 10);
      }).catch((getBufferHoursErr) => { reject(getBufferHoursErr); });

      await calcRevenueHour(empProjection, calcStartDate, calcStopDate, "", funcName).then((revenueHourArr) => {
         let revenueMonth = dateFormat(calcStartDate, "mmm-yyyy");
         let intRevenueHour = parseInt(revenueHourArr.revenueHour, 10) - intBufferHour;
         let intCmiRevenueHour = parseInt(revenueHourArr.cmiRevenueHour, 10);
         let intBillRatePerHr = parseInt(billRatePerHr, 10);
         let revenueAmount = intRevenueHour * intBillRatePerHr;
         let cmiRevenueAmount = intCmiRevenueHour * intBillRatePerHr;
         if (calcStartDate.getTime() >= sowStart.getTime() && calcStopDate.getTime() <= sowEnd.getTime()) {
            resolve({ "revenueMonth": revenueMonth, "revenueHour": intRevenueHour, "revenueAmount": revenueAmount, "cmiRevenueHour": intCmiRevenueHour, "cmiRevenueAmount": cmiRevenueAmount });
         } else { /* we aren't within SOW timeline */
            resolve({ "revenueMonth": revenueMonth, "revenueHour": 0, "revenueAmount": 0, "cmiRevenueHour": intCmiRevenueHour, "cmiRevenueAmount": cmiRevenueAmount });
         }
      });
   });
}


function computeRevenue(empProjection, revenueYear, callerName) {
   let monthRevArr = [];
   let funcName = computeRevenue.name;
   return new Promise((resolve, _reject) => {
      for (let monthIndex = 0; monthIndex <= 11; monthIndex++) {
         monthRevArr.push(getMonthlyRevenue(empProjection, revenueYear, monthIndex, funcName));
      }
      Promise.all(monthRevArr).then((monthRevenue) => {
         resolve(monthRevenue);
      });
   });
}

module.exports = {
   computeRevenue,
   getMonthlyRevenue
}