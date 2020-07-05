const dateTime = require("date-and-time");
const dateFormat = require("dateformat");


function getBufferHours(empBufferArr, monthIndex, callerName) {
   let funcName = getBufferHours.name;
   let bufferHours = 0;
   return new Promise(async (resolve, reject) => {
      if (empBufferArr === undefined || empBufferArr === "") {
         reject(funcName + ": Buffer array is not defined");
      } else if (monthIndex === undefined || monthIndex === "") {
         reject(funcName + ": Month index is not provided");
      } else {
         await empBufferArr.forEach((empBuffer, idx) => {
            if (idx < (empBufferArr.length - 1)) {
               let bufferMonth = new Date(dateTime.parse(empBuffer.month, "MMM-YYYY", true)).getMonth();
               if (monthIndex === bufferMonth) {
                  bufferHours = parseInt(empBuffer.hours, 10);
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
   return new Promise(async (resolve, reject) => {
      if ((leaveArr.length > 0) && (leaveDate !== undefined && leaveDate !== "") &&
         (wrkHrPerDay !== undefined && wrkHrPerDay !== "")) {
         let iWrkHourPerDay = parseInt(wrkHrPerDay, 10);
         await leaveArr.forEach((leave, idx) => {
            if (idx < (leaveArr.length - 1)) {
               let checkDate = new Date(leaveDate);
               checkDate.setHours(0, 0, 0, 0);
               let leaveStart = new Date(leave.startDate);
               leaveStart.setHours(0, 0, 0, 0);
               let leaveStop = new Date(leave.stopDate);
               leaveStop.setHours(0, 0, 0, 0);
               if (checkDate.getTime() >= leaveStart.getTime() && checkDate.getTime() <= leaveStop.getTime()) {
                  if (leave.halfDay !== undefined && leave.halfDay !== "" && leave.halfDay === "Y") {
                     if (leave.leaveHour === undefined || leave.leaveHour === "") {
                        if (iWrkHourPerDay === 1) {
                           leaveHour = iWrkHourPerDay;
                        } else if (iWrkHourPerDay > 1) {
                           if ((iWrkHourPerDay % 2) === 0) {
                              leaveHour = iWrkHourPerDay / 2;
                           } else {
                              leaveHour = (iWrkHourPerDay + 1) / 2;
                           }
                        }
                     } else {
                        leaveHour = parseInt(leave.leaveHour, 10);
                     }
                  } else if (leave.halfDay !== undefined && leave.halfDay !== "" && leave.halfDay === "N") {
                     leaveHour = iWrkHourPerDay;
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


function calcRevenueHour(wrkHrPerDay, monthlyDetail, startDate, stopDate, revenueHour, callerName) {
   let funcName = calcRevenueHour.name;
   return new Promise(async (resolve, _reject) => {
      let calcStartDate = new Date(startDate);
      calcStartDate.setHours(0, 0, 0, 0);
      let calcStopDate = new Date(stopDate);
      calcStopDate.setHours(0, 0, 0, 0);

      if (calcStartDate.getTime() > calcStopDate.getTime()) {
         return resolve(revenueHour);
      } else {
         let dayOfWeek = calcStartDate.getDay();
         let intSelfLeaveHour = 0;
         let intLocLeaveHour = 0;
         let intSelfAddlHour = 0;
         let intLocAddlHour = 0

         await computeLeaveHour(monthlyDetail["leaves"], calcStartDate, wrkHrPerDay, funcName).then((selfLeaveHour) => {
            intSelfLeaveHour = parseInt(selfLeaveHour, 10);
         });

         await computeLeaveHour(monthlyDetail["publicHolidays"], calcStartDate, wrkHrPerDay, funcName).then((locLeaveHour) => {
            intLocLeaveHour = parseInt(locLeaveHour, 10);
         });

         await computeLeaveHour(monthlyDetail["specialWorkDays"].empSplWrk, calcStartDate, wrkHrPerDay, funcName).then((selfAddlHour) => {
            intSelfAddlHour = parseInt(selfAddlHour, 10);
         });

         await computeLeaveHour(monthlyDetail["specialWorkDays"].locSplWrk, calcStartDate, wrkHrPerDay, funcName).then((locAddlHour) => {
            intLocAddlHour = parseInt(locAddlHour, 10);
         });

         if (dayOfWeek > 0 && dayOfWeek < 6) { /* weekday */
            if (intSelfLeaveHour === 0 && intLocLeaveHour === 0) { /* it's neither a personal leave nor a public holiday */
               revenueHour += wrkHrPerDay;
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
            } else if (intSelfLeaveHour === 0 && intLocLeaveHour > 0) { /* it's a public holiday and there is no personal leave that overlaps it */
               /* employee might have worked on this pubic holiday */
               if (intSelfAddlHour >= wrkHrPerDay) {
                  revenueHour += wrkHrPerDay;
               } else if (intSelfAddlHour > 0) {
                  revenueHour += intSelfAddlHour;
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
            }
         } else { /* weekend */
            if (intSelfAddlHour >= wrkHrPerDay) {
               revenueHour += wrkHrPerDay;
            } else if (intSelfAddlHour > 0) {
               revenueHour += intSelfAddlHour;
            }
         }

         calcStartDate.setDate(calcStartDate.getDate() + 1);
         return resolve(calcRevenueHour(wrkHrPerDay, monthlyDetail, calcStartDate, stopDate, revenueHour, funcName));
      }
   });
}



function calcCmiRevenueHour(wrkHrPerDay, monthlyDetail, startDate, stopDate, cmiRevenueHour, callerName) {
   let funcName = calcCmiRevenueHour.name;
   return new Promise(async (resolve, _reject) => {
      let calcStartDate = new Date(startDate);
      calcStartDate.setHours(0, 0, 0, 0);
      let calcStopDate = new Date(stopDate);
      calcStopDate.setHours(0, 0, 0, 0);

      if (calcStartDate.getTime() > calcStopDate.getTime()) {
         return resolve(cmiRevenueHour);
      } else {
         let dayOfWeek = calcStartDate.getDay();
         let intSelfLeaveHour = 0;
         let intLocLeaveHour = 0;
         let intSelfAddlHour = 0;
         let intLocAddlHour = 0

         await computeLeaveHour(monthlyDetail["leaves"], calcStartDate, wrkHrPerDay, funcName).then((selfLeaveHour) => {
            intSelfLeaveHour = parseInt(selfLeaveHour, 10);
         });

         await computeLeaveHour(monthlyDetail["publicHolidays"], calcStartDate, wrkHrPerDay, funcName).then((locLeaveHour) => {
            intLocLeaveHour = parseInt(locLeaveHour, 10);
         });

         await computeLeaveHour(monthlyDetail["specialWorkDays"].empSplWrk, calcStartDate, wrkHrPerDay, funcName).then((selfAddlHour) => {
            intSelfAddlHour = parseInt(selfAddlHour, 10);
         });

         await computeLeaveHour(monthlyDetail["specialWorkDays"].locSplWrk, calcStartDate, wrkHrPerDay, funcName).then((locAddlHour) => {
            intLocAddlHour = parseInt(locAddlHour, 10);
         });

         if (dayOfWeek > 0 && dayOfWeek < 6) { /* weekday */
            if (intSelfLeaveHour === 0 && intLocLeaveHour === 0) { /* it's neither a personal leave nor a public holiday */
               cmiRevenueHour += wrkHrPerDay;
            } else if (intSelfLeaveHour > 0 && intLocLeaveHour === 0) { /* it's a personal leave and not a public holiday */
               cmiRevenueHour += wrkHrPerDay;
            } else if (intSelfLeaveHour === 0 && intLocLeaveHour > 0) {
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
            if (intLocAddlHour >= wrkHrPerDay) {
               cmiRevenueHour += wrkHrPerDay;
            } else if (intLocAddlHour > 0) {
               cmiRevenueHour += intLocAddlHour;
            }
         }

         calcStartDate.setDate(calcStartDate.getDate() + 1);
         return resolve(calcCmiRevenueHour(wrkHrPerDay, monthlyDetail, calcStartDate, stopDate, cmiRevenueHour, funcName));
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

      let calcStartDate = monthFirstDate;
      let calcStopDate = monthLastDate;
      let forecastStopDate = sowEnd;

      if (sowStart.getFullYear() === calcStartDate.getFullYear() && sowStart.getMonth() === calcStartDate.getMonth()) {
         calcStartDate = sowStart;
      }

      if (sowEnd.getFullYear() === calcStopDate.getFullYear() && sowEnd.getMonth() === calcStopDate.getMonth()) {
         calcStopDate = sowEnd;
      }

      if (empProjection[0].foreseenSowStop !== undefined && empProjection[0].foreseenSowStop !== "") {
         forecastStopDate = new Date(dateTime.parse(empProjection[0].foreseenSowStop, "D-MMM-YYYY", true));
         forecastStopDate.setHours(0, 0, 0, 0);

         if (forecastStopDate.getFullYear() === calcStopDate.getFullYear()) {
            if (forecastStopDate.getMonth() === calcStopDate.getMonth()) {
               calcStopDate = forecastStopDate;
            } else if (forecastStopDate.getMonth() > calcStopDate.getMonth()) {
               calcStopDate = monthLastDate;
            }
         } else if (forecastStopDate.getFullYear() > calcStopDate.getFullYear()) {
            calcStopDate = monthLastDate;
         }
      }

      calcStartDate.setHours(0, 0, 0, 0);
      calcStopDate.setHours(0, 0, 0, 0);

      let monthlyDetail = empProjection[1].monthlyDetail[monthIndex];
      let iBufferHour = 0;
      if (monthlyDetail.length > 0) {
         iBufferHour = monthlyDetail["buffers"][0].hours;
      }
      await calcRevenueHour(wrkHrPerDay, monthlyDetail, calcStartDate, calcStopDate, 0, funcName).then(async (revenueHour) => {
         await calcCmiRevenueHour(wrkHrPerDay, monthlyDetail, monthFirstDate, monthLastDate, 0, funcName).then((cmiRevenueHour) => {
            let revenueMonth = dateFormat(calcStartDate, "mmm-yyyy");
            let iRevenueHour = parseInt(revenueHour, 10) - iBufferHour;
            let iCmiRevenueHour = parseInt(cmiRevenueHour, 10);
            let iBillRatePerHr = parseInt(billRatePerHr, 10);
            let iRevenueAmount = iRevenueHour * iBillRatePerHr;
            let iCmiRevenueAmount = iCmiRevenueHour * iBillRatePerHr;
            if (calcStartDate.getTime() >= sowStart.getTime() && (calcStopDate.getTime() <= sowEnd.getTime() || calcStopDate.getTime() <= forecastStopDate.getTime())) {
               resolve({ "revenueMonth": revenueMonth, "revenueHour": iRevenueHour, "revenueAmount": iRevenueAmount, "cmiRevenueHour": iCmiRevenueHour, "cmiRevenueAmount": iCmiRevenueAmount });
            } else { /* we aren't within SOW timeline */
               resolve({ "revenueMonth": revenueMonth, "revenueHour": 0, "revenueAmount": 0, "cmiRevenueHour": iCmiRevenueHour, "cmiRevenueAmount": iCmiRevenueAmount });
            }
         });
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