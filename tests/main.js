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


 // This code should update the categories according to the new API.

 d3.json('data/combined_flights_data.json').then(function(data) {
    // Process data
    data.forEach(function(d) {
        // Assuming the date is in d.arrival.actualTime or d.departure.actualTime
        let actualTime = d.arrival ? d.arrival.actualTime : (d.departure ? d.departure.actualTime : null);

        if (actualTime) {
            let date = new Date(actualTime); // Parsing ISO 8601 date format
            d.latestTime = date;
            d.Hour = date.getHours();
            d.Minute = date.getMinutes();

            d.Time_Category = "Regular arrivals";
            if (d.Hour === 23 && d.Minute < 30) d.Time_Category = "Shoulder hour flights";
            else if ((d.Hour === 23 && d.Minute >= 30) || d.Hour < 6) d.Time_Category = "Night hour arrivals";
            else if (d.Hour === 6) d.Time_Category = "Shoulder hour flights";
        }
    });

    let total_flights = data.length;
    let shoulder_hour_flights = data.filter(d => d.Time_Category === 'Shoulder hour flights').length;
    let night_hour_flights = data.filter(d => d.Time_Category === 'Night hour arrivals').length;

    // Quotas
    let quotas = [85990, 9500, 4000];

    // Categories and counts
    let categories = ['Total Flights', 'Shoulder Hour Flights', 'Night Hour Flights'];
    let counts = [total_flights, shoulder_hour_flights, night_hour_flights];

    // Calculating percentages
    let percentages = counts.map((count, index) => (count / quotas[index]) * 100);

 })