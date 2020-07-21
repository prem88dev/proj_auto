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


function computeSelfLocLeaveOrLocSplWorkHour(referenceArr, leaveDate, workHourPerDay, callerName) {
   let funcName = computeSelfLocLeaveOrLocSplWorkHour.name;
   let iLeaveHour = 0;
   let iWorkHourPerDay = 0;
   if (workHourPerDay !== undefined && workHourPerDay !== "") {
      iWorkHourPerDay = parseInt(workHourPerDay, 10);
   }
   return new Promise(async (resolve, reject) => {
      if (referenceArr !== undefined && referenceArr.length > 0 && leaveDate !== undefined && leaveDate !== "") {
         await referenceArr.forEach((leave) => {
            let checkDate = new Date(leaveDate);
            checkDate.setHours(0, 0, 0, 0);
            let leaveStart = new Date(leave.startDate);
            leaveStart.setHours(0, 0, 0, 0);
            let leaveStop = new Date(leave.stopDate);
            leaveStop.setHours(0, 0, 0, 0);
            if (checkDate.getTime() >= leaveStart.getTime() && checkDate.getTime() <= leaveStop.getTime()) {
               if (leave.halfDay !== undefined && leave.halfDay !== "" && leave.halfDay === "Y") {
                  if (leave.leaveHour === undefined || leave.leaveHour === "") {
                     if ((iWorkHourPerDay % 2) === 0) {
                        iLeaveHour = iWorkHourPerDay / 2;
                     } else {
                        iLeaveHour = (iWorkHourPerDay + 1) / 2;
                     }
                  } else {
                     iLeaveHour = parseInt(leave.leaveHour, 10);
                  }
               } else if (leave.halfDay !== undefined && leave.halfDay !== "" && leave.halfDay === "N") {
                  iLeaveHour = iWorkHourPerDay;
               }
            }
         });
         resolve(iLeaveHour);
      } else {
         resolve(iLeaveHour);
      }
   });
}


function computeEmpExtraWorkHour(empSplWorkArr, workDate, workHourPerDay, callerName) {
   let funcName = computeEmpExtraWorkHour.name;
   let iExtraWorkHour = 0;
   let iWorkHourPerDay = 0;
   if (workHourPerDay !== undefined && workHourPerDay !== "") {
      iWorkHourPerDay = parseInt(workHourPerDay, 10);
   }
   return new Promise(async (resolve, reject) => {
      if (empSplWorkArr !== undefined && empSplWorkArr.length > 0 && workDate !== undefined && workDate !== "") {
         await empSplWorkArr.forEach((extraWork) => {
            let checkDate = new Date(workDate);
            checkDate.setHours(0, 0, 0, 0);
            let extraWorkStart = new Date(extraWork.startDate);
            extraWorkStart.setHours(0, 0, 0, 0);
            let extraWorkStop = new Date(extraWork.stopDate);
            extraWorkStop.setHours(0, 0, 0, 0);
            if (checkDate.getTime() >= extraWorkStart.getTime() && checkDate.getTime() <= extraWorkStop.getTime()) {
               iExtraWorkHour += extraWork.workHour;
            }
         });
         resolve(iExtraWorkHour);
      } else {
         resolve(iExtraWorkHour);
      }
   });
}


function calcRevenueHour(workHourPerDay, monthlyDetail, startDate, stopDate, revenueHour, callerName) {
   let funcName = calcRevenueHour.name;
   return new Promise(async (resolve, reject) => {
      let calcStartDate = new Date(startDate);
      calcStartDate.setHours(0, 0, 0, 0);
      let calcStopDate = new Date(stopDate);
      calcStopDate.setHours(0, 0, 0, 0);

      if (calcStartDate.getTime() > calcStopDate.getTime()) {
         return resolve(revenueHour);
      } else {
         let dayOfWeek = calcStartDate.getDay();
         let iSelfLeaveHour = 0;
         let iSelfAddlHour = 0;
         let iLocLeaveHour = 0;
         let iLocAddlHour = 0;

         await computeSelfLocLeaveOrLocSplWorkHour(monthlyDetail[0].leaves, calcStartDate, workHourPerDay, funcName).then((selfLeaveHour) => {
            iSelfLeaveHour = parseInt(selfLeaveHour, 10);
         });

         await computeEmpExtraWorkHour(monthlyDetail[0].specialWorkDays["empSplWrk"], calcStartDate, workHourPerDay, funcName).then((selfAddlHour) => {
            iSelfAddlHour = parseInt(selfAddlHour, 10);
         });

         await computeSelfLocLeaveOrLocSplWorkHour(monthlyDetail[0].publicHolidays, calcStartDate, workHourPerDay, funcName).then((locLeaveHour) => {
            iLocLeaveHour = parseInt(locLeaveHour, 10);
         });

         await computeSelfLocLeaveOrLocSplWorkHour(monthlyDetail[0].specialWorkDays["locSplWrk"], calcStartDate, workHourPerDay, funcName).then((locAddlHour) => {
            iLocAddlHour = parseInt(locAddlHour, 10);
         });

         if (dayOfWeek > 0 && dayOfWeek < 6 && iLocLeaveHour === 0) { /* weekday and not a location holiday */
            if (iSelfLeaveHour === 0 || iSelfAddlHour >= iSelfLeaveHour) {
               /* not personal leave or additional hours and leave hours cancel each other */
               revenueHour += workHourPerDay;
            } else if (iSelfLeaveHour > 0 && iSelfAddlHour === 0 && iSelfLeaveHour < workHourPerDay) { /* personal leave */
               revenueHour += (workHourPerDay - iSelfLeaveHour);
            } else if (iSelfLeaveHour > 0 && iSelfAddlHour > 0 && iSelfAddlHour < iSelfLeaveHour) { /* over-riding personal leave */
               if ((iSelfLeaveHour - iSelfAddlHour) <= workHourPerDay) {
                  revenueHour += workHourPerDay - (iSelfLeaveHour - iSelfAddlHour)
               }
            }
         } else if (dayOfWeek > 0 && dayOfWeek < 6 && iLocLeaveHour > 0) { /* location holiday on a weekday */
            let effWorkHour = iLocAddlHour;
            if (effWorkHour >= workHourPerDay || effWorkHour === 0) {
               effWorkHour = workHourPerDay;
            }

            if (effWorkHour > iLocLeaveHour) {
               revenueHour += (effWorkHour - iLocLeaveHour);
            }
         } else if (dayOfWeek === 0 || dayOfWeek === 6) { /* weekend with additional work hours */
            if ((iSelfAddlHour + iLocAddlHour) >= workHourPerDay) {
               revenueHour += workHourPerDay;
            } else if ((iSelfAddlHour + iLocAddlHour) < workHourPerDay) {
               revenueHour += (iSelfAddlHour + iLocAddlHour);
            }
         }

         calcStartDate.setDate(calcStartDate.getDate() + 1);
         return resolve(calcRevenueHour(workHourPerDay, monthlyDetail, calcStartDate, stopDate, revenueHour, funcName));
      }
   });
}


function calcCmiRevenueHour(workHourPerDay, monthlyDetail, startDate, stopDate, cmiRevenueHour, callerName) {
   let funcName = calcCmiRevenueHour.name;
   return new Promise(async (resolve, reject) => {
      let calcStartDate = new Date(startDate);
      calcStartDate.setHours(0, 0, 0, 0);
      let calcStopDate = new Date(stopDate);
      calcStopDate.setHours(0, 0, 0, 0);

      if (calcStartDate.getTime() > calcStopDate.getTime()) {
         return resolve(cmiRevenueHour);
      } else {
         let dayOfWeek = calcStartDate.getDay();
         let iLocLeaveHour = 0;
         let iLocAddlHour = 0;

         await computeSelfLocLeaveOrLocSplWorkHour(monthlyDetail[0].publicHolidays, calcStartDate, workHourPerDay, funcName).then((locLeaveHour) => {
            iLocLeaveHour = parseInt(locLeaveHour, 10);
         });

         await computeSelfLocLeaveOrLocSplWorkHour(monthlyDetail[0].specialWorkDays["locSplWrk"], calcStartDate, workHourPerDay, funcName).then((locAddlHour) => {
            iLocAddlHour = parseInt(locAddlHour, 10);
         });

         if (dayOfWeek > 0 && dayOfWeek < 6 && iLocLeaveHour === 0) { /* weekday */
            cmiRevenueHour += workHourPerDay;
         } else if (dayOfWeek > 0 && dayOfWeek < 6 && iLocLeaveHour > 0) { /* over-riding  location holiday */
            if (iLocAddlHour >= iLocLeaveHour && iLocAddlHour >= workHourPerDay) {
               cmiRevenueHour += workHourPerDay;
            } else if (iLocAddlHour < iLocLeaveHour) { /* location holiday. but has been made a working day */
               cmiRevenueHour += ((workHourPerDay - iLocLeaveHour) + iLocAddlHour);
            }
         } else if (dayOfWeek === 0 || dayOfWeek === 6) {
            if (iLocAddlHour >= workHourPerDay) {
               cmiRevenueHour += workHourPerDay;
            } else {
               cmiRevenueHour += iLocAddlHour;
            }
         }

         calcStartDate.setDate(calcStartDate.getDate() + 1);
         return resolve(calcCmiRevenueHour(workHourPerDay, monthlyDetail, calcStartDate, stopDate, cmiRevenueHour, funcName));
      }
   });
}


function getMonthlyRevenue(empProjection, monthlyDetail, revenueYear, monthIndex, callerName) {
   let funcName = getMonthlyRevenue.name;
   return new Promise(async (resolve, reject) => {
      let iWorkHourPerDay = parseInt(empProjection[0].workHourPerDay, 10);
      let iBillRatePerHour = parseInt(empProjection[0].billRatePerHour, 10);
      let calcYear = parseInt(revenueYear, 10);
      let calcStartMonth = parseInt(monthIndex, 10);
      let calcStopMonth = parseInt(monthIndex, 10) + 1;

      let monthFirstDate = new Date(calcYear, calcStartMonth, 1);
      monthFirstDate.setHours(0, 0, 0, 0);
      let monthLastDate = new Date(calcYear, calcStopMonth, 0);
      monthLastDate.setHours(0, 0, 0, 0);

      let sowStart = new Date(dateTime.parse(empProjection[0].sowStart, "D-MMM-YYYY", true));
      sowStart.setHours(0, 0, 0, 0);
      let sowStop = new Date(dateTime.parse(empProjection[0].sowStop, "D-MMM-YYYY", true));
      sowStop.setHours(0, 0, 0, 0);

      let calcStartDate = monthFirstDate;
      let calcStopDate = monthLastDate;
      let forecastStopDate = sowStop;

      if (sowStart.getFullYear() === calcStartDate.getFullYear() && sowStart.getMonth() === calcStartDate.getMonth()) {
         calcStartDate = sowStart;
      }

      if (sowStop.getFullYear() === calcStopDate.getFullYear() && sowStop.getMonth() === calcStopDate.getMonth()) {
         calcStopDate = sowStop;
      }

      if (empProjection[0].forecastSowStop !== undefined && empProjection[0].forecastSowStop !== "") {
         forecastStopDate = new Date(dateTime.parse(empProjection[0].forecastSowStop, "D-MMM-YYYY", true));
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

      let iBufferHour = 0;
      if (monthlyDetail.length > 0 && monthlyDetail["buffers"] !== undefined || monthlyDetail["buffers"] !== "") {
         if (monthlyDetail[0]["buffers"].length > 0) {
            iBufferHour = monthlyDetail[0]["buffers"][0].hours;
         }
      }
      await calcRevenueHour(iWorkHourPerDay, monthlyDetail, calcStartDate, calcStopDate, 0, funcName).then(async (revenueHour) => {
         await calcCmiRevenueHour(iWorkHourPerDay, monthlyDetail, monthFirstDate, monthLastDate, 0, funcName).then((cmiRevenueHour) => {
            let revenueMonth = dateFormat(calcStartDate, "mmm-yyyy");
            let iRevenueHour = parseInt(revenueHour, 10) - iBufferHour;
            let iCmiRevenueHour = parseInt(cmiRevenueHour, 10);
            let iRevenueAmount = iRevenueHour * iBillRatePerHour;
            let iCmiRevenueAmount = iCmiRevenueHour * iBillRatePerHour;
            if (calcStartDate.getTime() >= sowStart.getTime() && (calcStopDate.getTime() <= sowStop.getTime() || calcStopDate.getTime() <= forecastStopDate.getTime())) {
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
   return new Promise((resolve, reject) => {
      for (let monthIndex = 0; monthIndex <= 11; monthIndex++) {
         let monthlyDetail = empProjection[1].monthlyDetail[monthIndex];
         monthRevArr.push(getMonthlyRevenue(empProjection, monthlyDetail, revenueYear, monthIndex, funcName));
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