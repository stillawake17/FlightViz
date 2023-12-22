// Function to aggregate flight data by month for a given year
function aggregateDataByMonth(flightData, year) {
    const monthlyCounts = new Array(12).fill(0); // Array for each month
    
    flightData.forEach(d => {
        // Assuming the date is in d.arrival.actualTime or d.departure.actualTime
        let actualTime = d.arrival ? d.arrival.actualTime : (d.departure ? d.departure.actualTime : null);
        
        if (actualTime) {
            let date = new Date(actualTime);
            let flightYear = date.getFullYear();
            let flightMonth = date.getMonth(); // getMonth() returns 0-11 for Jan-Dec
            
            if (flightYear === year) {
                monthlyCounts[flightMonth]++;
            }
        }
    });

    return monthlyCounts;
}


//TO TEST
// Example usage with mock data
 let FlightData = 'data\\combined_flights_data.json'

 console.log(aggregateDataByMonth(FlightData, 2023));
