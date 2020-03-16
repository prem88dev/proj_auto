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




module.exports = {
   getDaysBetween
}