const commObj = require("./utility");
const splWrkObj = require("./specialWorkday");
const dateTime = require("date-and-time");
const dateFormat = require("dateformat");


function calcMonthLeaves(leaveArrObj, monthIndex, startDate, stopDate) {
   return new Promise(async (resolve, reject) => {
      if (leaveArrObj === undefined || leaveArrObj === "") {
         reject(calcMonthLeaves.name + ": Employee leave array is not provided");
      } else if (monthIndex === undefined || monthIndex === "") {
         reject(calcMonthLeaves.name + ": Month index is not provided");
      } else if (startDate === undefined || startDate === "") {
         reject(calcMonthLeaves.name + ": Revenue month start date is not provided");
      } else if (stopDate === undefined || stopDate === "") {
         reject(calcMonthLeaves.name + ": Revenue month end date is not provided");
      } else {
         let leaveDays = 0;
         let totalObjs = (leaveArrObj.length - 1);
         await leaveArrObj.forEach((leaveObj, idx) => {
            if (idx < totalObjs) {
               if (leaveObj.startDate !== undefined && leaveObj.startDate != "" && leaveObj.endDate !== undefined && leaveObj.endDate != "") {
                  let leaveStart = new Date(leaveObj.startDate);
                  leaveStart.setHours(0, 0, 0, 0);
                  let leaveStop = new Date(leaveObj.endDate);
                  leaveStop.setHours(25, 59, 59, 0);

                  commObj.getDaysBetween(leaveStart, leaveStop, true).then((daysBetween) => {
                     leaveDays += parseInt(daysBetween, 10);
                  });
               }
            }
         });
         resolve(leaveDays);
      }
   });
}


function getBufferHours(empBufferArr, monthIndex, wrkHrPerDay, callerName) {
   let bufferHours = 0;
   return new Promise(async (resolve, reject) => {
      if (empBufferArr === undefined || empBufferArr === "") {
         reject(getBufferHours.name + ": Buffer array is not defined");
      } else if (monthIndex === undefined || monthIndex === "") {
         reject(getBufferHours.name + ": Month index is not provided");
      } else if (wrkHrPerDay === undefined || wrkHrPerDay === "") {
         reject(getBufferHours.name + ": Billing hours per day is not provided");
      } else {
         await empBufferArr.forEach((empBuffer) => {
            let bufferMonth = new Date(dateTime.parse(empBuffer.month, "MMM-YYYY", true)).getMonth();
            if (monthIndex === bufferMonth) {
               bufferHours = parseInt(empBuffer.days, 10) * parseInt(wrkHrPerDay, 10);
            }
         });
         resolve(bufferHours);
      }
   });
}


function computeHour(leaveArr, leaveDate, wrkHrPerDay, callerName) {
   return new Promise(async (resolve, _reject) => {
      let leaveHour = 0;
      if (leaveArr.length > 0) {
         await leaveArr.forEach((leave, idx) => {
            let checkDate = new Date(leaveDate);
            checkDate.setHours(0, 0, 0, 0);
            let leaveStart = new Date(leave.startDate);
            leaveStart.setHours(0, 0, 0, 0);
            let leaveStop = new Date(leave.endDate);
            leaveStop.setHours(0, 0, 0, 0);
            if (checkDate.getTime() >= leaveStart.getTime() && checkDate.getTime() <= leaveStop.getTime()) {
               if (leave.halfDayInd === 1) {
                  leaveHour = leave.leaveHour;
               } else {
                  leaveHour = wrkHrPerDay;
               }
            }
         });
      }
      resolve(leaveHour);
   });
}


const calcRevenueHour = (empJsonObj, startDate, stopDate, revenueHourArr, callerName) => {
   let funcName = calcRevenueHour.name;
   let wrkHrPerDay = 0;
   if (empJsonObj[0].wrkHrPerDay !== undefined && empJsonObj[0].wrkHrPerDay !== "") {
      wrkHrPerDay = parseInt(empJsonObj[0].wrkHrPerDay, 10);
   }
   return new Promise((resolve, _reject) => {
      let revenueHour = 0;
      let cmiRevenueHour = 0;
      let refStartDate = new Date(startDate);
      refStartDate.setHours(0, 0, 0, 0);
      let refStopDate = new Date(stopDate);
      refStopDate.setHours(0, 0, 0, 0);
      let tmpHourArr = { "revenueHour": revenueHour, "cmiRevenueHour": cmiRevenueHour, "nextStartDate": stopDate };

      if (refStartDate.getTime() <= refStopDate.getTime()) {
         if (revenueHourArr !== undefined && revenueHourArr !== "") {
            if (revenueHourArr.revenueHour !== undefined && revenueHourArr.revenueHour !== "") {
               revenueHour = parseInt(revenueHourArr.revenueHour, 10);
            }
            if (revenueHourArr.cmiRevenueHour !== undefined && revenueHourArr.cmiRevenueHour !== "") {
               cmiRevenueHour = parseInt(revenueHourArr.cmiRevenueHour, 10);
            }
            if (revenueHourArr.nextStartDate !== undefined && revenueHourArr.nextStartDate !== "") {
               refStartDate = new Date(revenueHourArr.nextStartDate);
               refStartDate.setHours(0, 0, 0, 0);
            }
         }

         let dayOfWeek = refStartDate.getDay();
         let intSelfLeaveHour = 0;
         let intLocLeaveHour = 0;
         let intEmpAddlWrkHour = 0;
         let intLocAddlWrkHour = 0

         computeHour(empJsonObj[1].leaves, refStartDate, wrkHrPerDay, funcName).then((selfLeaveHour) => {
            computeHour(empJsonObj[3].publicHolidays, refStartDate, wrkHrPerDay, funcName).then((locLeaveHour) => {
               computeHour(empJsonObj[4].specialWorkDays.empSplWrk, refStartDate, 0, funcName).then((empAddlWrkHour) => {
                  computeHour(empJsonObj[4].specialWorkDays.locSplWrk, refStartDate, 0, funcName).then((locAddlWrkHour) => {
                     intSelfLeaveHour = parseInt(selfLeaveHour, 10);
                     intLocLeaveHour = parseInt(locLeaveHour, 10);
                     intEmpAddlWrkHour = parseInt(empAddlWrkHour, 10);
                     intLocAddlWrkHour = parseInt(locAddlWrkHour, 10);

                     if ((dayOfWeek > 0 && dayOfWeek < 6) ||
                        ((dayOfWeek === 0 || dayOfWeek === 6) && (intLocAddlWrkHour > 0 || intEmpAddlWrkHour > 0))) {
                        if (intSelfLeaveHour > 0 || intLocLeaveHour > 0) {
                           let effEmpLeaveHour = intSelfLeaveHour; /* intSelfLeaveHour - 4, intEmpAddlWrkHour - 5 */
                           if (intEmpAddlWrkHour <= intSelfLeaveHour) {
                              effEmpLeaveHour = intSelfLeaveHour - intEmpAddlWrkHour;
                           } else {
                              effEmpLeaveHour = 0;
                           } /* effEmpLeaveHour - 0 */

                           let effLocLeaveHour = intLocLeaveHour; /* intLocLeaveHour - 4, intLocAddlWrkHour - 2 */
                           if (intLocAddlWrkHour <= intLocLeaveHour) {
                              effLocLeaveHour = intLocLeaveHour - intLocAddlWrkHour;
                           } else {
                              effLocLeaveHour = 0;
                           } /* effLocLeaveHour - 2 */

                           let effLeaveHour = wrkHrPerDay;
                           if ((effEmpLeaveHour + effLocLeaveHour) < effLeaveHour) {
                              effLeaveHour -= effEmpLeaveHour + effLocLeaveHour;
                           } /* effLeaveHour - 2 */

                           if (intEmpAddlWrkHour > 0 && intLocAddlWrkHour > 0) {
                              let effWorkHour = wrkHrPerDay;
                              if ((intEmpAddlWrkHour + intLocAddlWrkHour) < wrkHrPerDay) {
                                 effWorkHour = intEmpAddlWrkHour + intLocAddlWrkHour;
                              } /* effWorkHour - 7 */

                              if (effLeaveHour < effWorkHour) {
                                 revenueHour += effWorkHour - effLeaveHour;
                              }

                              if (effLocLeaveHour < wrkHrPerDay) {
                                 cmiRevenueHour += wrkHrPerDay - effLocLeaveHour;
                              }
                           } else if (intLocAddlWrkHour > 0) {
                              if (intSelfLeaveHour < wrkHrPerDay) {
                                 revenueHour += wrkHrPerDay - intSelfLeaveHour;
                              }

                              if (effLocLeaveHour <= wrkHrPerDay) {
                                 cmiRevenueHour += wrkHrPerDay - effLocLeaveHour;
                              }
                           } else if (intEmpAddlWrkHour > 0) {
                              if (effEmpLeaveHour < wrkHrPerDay) {
                                 revenueHour += wrkHrPerDay - effEmpLeaveHour;
                              }

                              if (intLocLeaveHour <= wrkHrPerDay) {
                                 cmiRevenueHour += wrkHrPerDay - intLocLeaveHour;
                              }
                           } else {
                              if (intSelfLeaveHour <= wrkHrPerDay) {
                                 revenueHour += wrkHrPerDay - intSelfLeaveHour;
                              }

                              if (intLocLeaveHour <= wrkHrPerDay) {
                                 cmiRevenueHour += wrkHrPerDay - intLocLeaveHour;
                              }
                           }
                        } else if (dayOfWeek > 0 && dayOfWeek < 6) {
                           revenueHour += wrkHrPerDay;
                           cmiRevenueHour += wrkHrPerDay;
                        } else if (dayOfWeek === 0 || dayOfWeek === 6) {
                           revenueHour += intEmpAddlWrkHour + intLocAddlWrkHour;
                           cmiRevenueHour += intEmpAddlWrkHour + intLocAddlWrkHour;
                        }
                     }
                  });
               });
            });
         });

         refStartDate.setDate(refStartDate.getDate() + 1);
         tmpHourArr = { "revenueHour": revenueHour, "cmiRevenueHour": cmiRevenueHour, "nextStartDate": refStartDate };

         if (refStartDate.getTime() > refStopDate.getTime()) {
            return resolve(tmpHourArr)
         } else {
            return resolve(calcRevenueHour(empJsonObj, refStartDate, stopDate, tmpHourArr, funcName));
         }
      }
   });
}


/* var calcRevenueHour = (empJsonObj, startDate, stopDate, revenueHourArr, callerName) => {
   let funcName = calcRevenueHour.name;
   return recurseCalcRevenueHour(empJsonObj, startDate, stopDate, revenueHourArr, funcName).then((revenueHour) => {
      return new Promise((resolve, _reject) => {
         let refStartDate = new Date(revenueHour.nextStartDate);
         refStartDate.setUTCHours(0, 0, 0, 0);
         let refStopDate = new Date(stopDate);
         refStopDate.setUTCHours(0, 0, 0, 0);

         if (refStartDate.getTime() > refStopDate.getTime()) {
            resolve(revenueHour);
         } else {
            return resolve(calcRevenueHour(empJsonObj, refStartDate, stopDate, revenueHour, funcName));
         }
      });
   });
} */


function getEmpMonthlyRevenue(empJsonObj, startDate, stopDate) {
   return new Promise((resolve, reject) => {
      if (empJsonObj === undefined || empJsonObj === "") {
         reject(calcMonthLeaves.name + ": Employee leave array is not provided");
      } else if (startDate === undefined || startDate === "") {
         reject(calcMonthLeaves.name + ": Revenue month start date is not provided");
      } else if (stopDate === undefined || stopDate === "") {
         reject(calcMonthLeaves.name + ": Revenue month end date is not provided");
      } else {
         let sowStart = new Date(dateTime.parse(empJsonObj[0].sowStartDate, "D-MMM-YYYY", true));
         sowStart.setHours(0, 0, 0, 0);
         let sowEnd = new Date(dateTime.parse(empJsonObj[0].sowEndDate, "D-MMM-YYYY", true));
         sowEnd.setHours(0, 0, 0, 0);
         let empEsaLink = empJsonObj[0].empEsaLink;
         let strCtsEmpId = `${empJsonObj[0].ctsEmpId}`;
         let cityCode = empJsonObj[0].cityCode;
         let billRatePerHr = parseInt(empJsonObj[0].billRatePerHr, 10);
         let wrkHrPerDay = parseInt(empJsonObj[0].wrkHrPerDay, 10);

         let revenueStartDate = new Date(startDate);
         revenueStartDate.setUTCHours(0, 0, 0, 0);
         let revenueStopDate = new Date(stopDate);
         revenueStopDate.setUTCHours(0, 0, 0, 0);

         let refStartDate = new Date(startDate);
         refStartDate.setHours(0, 0, 0, 0);
         let refStopDate = new Date(stopDate);
         refStopDate.setHours(0, 0, 0, 0);

         if (sowStart.getFullYear() === refStartDate.getFullYear()) {
            if (sowStart.getMonth() === refStartDate.getMonth()) {
               refStartDate = sowStart;
            }
         }

         if (sowEnd.getFullYear() === refStopDate.getFullYear()) {
            if (sowEnd.getMonth() === refStopDate.getMonth()) {
               refStopDate = sowEnd;
            }
         }

         let strStartDate = dateFormat(refStartDate, "yyyy-mm-dd") + "T00:00:00.000Z";
         let strStopDate = dateFormat(refStopDate, "yyyy-mm-dd") + "T00:00:00.000Z";

         calcRevenueHour(empJsonObj, strStartDate, strStopDate, "", getEmpMonthlyRevenue.name).then((revenueHourArr) => {
            let revenueMonth = dateFormat(refStartDate, "mmm-yyyy");
            let intRevenueHour = parseInt(revenueHourArr.revenueHour, 10);
            let intCmiRevenueHour = parseInt(revenueHourArr.cmiRevenueHour, 10);
            resolve({ "revenueMonth": revenueMonth, "revenueHour": intRevenueHour, "cmiRevenueHour": intCmiRevenueHour })
         });

         /*commObj.getWeekDaysBetween(refStartDate, refStopDate, true, getEmpMonthlyRevenue.name).then((revenueDays) => {
            splWrkObj.getSplWrkWkEndHrs(empEsaLink, strCtsEmpId, cityCode, wrkHrPerDay, strStartDate, strStopDate, getEmpMonthlyRevenue.name).then((weekendWorHours) => {
               let totalWorkHours = (revenueDays * wrkHrPerDay) + weekendWorHours;
               getBufferHours(empJsonObj[2].buffers, monthIndex, wrkHrPerDay).then((bufferHours) => {
                  commObj.calcLeaveHours(empJsonObj[0], strStartDate, strStopDate, getEmpMonthlyRevenue.name).then((effWrkHrs) => {
                     let monthRevenue = 0;
                     let cmiRevenue = 0;
                     let monthRevenueObj = {};
                     let revWorkDays = parseInt(revMonthWorkDays, 10);
                     let locationHolidays = parseInt(locationLeaves, 10);
                     let selfDays = parseInt(personalDays, 10);
                     let buffer = parseInt(bufferDays, 10);
                     let revenueDays = revWorkDays - (locationHolidays + selfDays + buffer);
                     let cmiRevenueDays = revWorkDays - locationHolidays;
                     if (refStartDate >= sowStart && refStopDate <= sowEnd) {
                        monthRevenue = revenueDays * wrkHrPerDay * billRatePerHr;
                        cmiRevenue = cmiRevenueDays * wrkHrPerDay * billRatePerHr;
                     }
                     monthRevenueObj = { 'month': dateFormat(dateTime.parse(revenueMonth, "MMYYYY", true), "mmm-yyyy"), 'firstDate': dateFormat(revenueStartDate, "d-mmm-yyyy"), 'lastDate': dateFormat(revenueStopDate, "d-mmm-yyyy"), 'revWorkDays': revWorkDays, 'locationHolidays': locationHolidays, 'selfDays': selfDays, 'bufferDays': buffer, 'monthRevenue': monthRevenue, 'cmiRevenue': cmiRevenue };
                     resolve(monthRevenueObj);
                  }).catch((calcMonthBuffersErr) => { reject(calcMonthBuffersErr); });
               }).catch((countLocationHolidaysErr) => { reject(countLocationHolidaysErr); });
            });
         }).catch((countPersonalDaysErr) => { reject(countPersonalDaysErr); });*/
      }
   });
}


function calcEmpRevenue(empJsonObj, revenueYear) {
   let monthRevArr = [];
   return new Promise((resolve, reject) => {
      if (empJsonObj === undefined) {
         reject(calcEmpRevenue.name + ": Employee object is not provided");
      } else if (revenueYear === undefined) {
         reject(calcEmpRevenue.name + ": Revenue year is not provided");
      } else if (empJsonObj[0].sowStartDate === undefined || empJsonObj[0].sowStartDate == "") {
         reject(calcEmpRevenue.name + ": SOW start date is not defined for selected employee");
      } else if (empJsonObj[0].sowEndDate === undefined || empJsonObj[0].sowEndDate == "") {
         reject(calcEmpRevenue.name + ": SOW end date is not defined for selected employee");
      } else if (empJsonObj[0].billRatePerHr === undefined || empJsonObj[0].billRatePerHr == "") {
         reject(calcEmpRevenue.name + ": Billing hour per day is not defined for selected employee");
      } else if (empJsonObj[0].wrkHrPerDay === undefined || empJsonObj[0].wrkHrPerDay == "") {
         reject(calcEmpRevenue.name + ": Billing rate is not defined for selected employee");
      } else {
         for (let monthIndex = 0; monthIndex <= 11; monthIndex++) {
            let startDate = new Date(revenueYear, monthIndex, 1);
            startDate.setHours(0, 0, 0, 0);
            let stopDate = new Date(revenueYear, (monthIndex + 1), 0);
            stopDate.setHours(0, 0, 0, 0);
            monthRevArr.push(getEmpMonthlyRevenue(empJsonObj, startDate, stopDate));
         }
      }
      Promise.all(monthRevArr).then((monthRevenue) => {
         resolve(monthRevenue);
      });
   });
}

module.exports = {
   calcEmpRevenue,
   getEmpMonthlyRevenue
}