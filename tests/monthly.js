// Define the URL or path to your JSON file
const jsonFilePath = '../data/combined_flights_data.json'; // Use a relative path

// Load the JSON data using D3.js
d3.json(jsonFilePath).then(function(data) {
    // Process the data to get flight counts per month
    const flightCounts = data.reduce((acc, flight) => {
        if (flight.departure && flight.departure.actualTime) {
            // Extract the year-month part from the actualTime
            const yearMonth = flight.departure.actualTime.substring(0, 7);
            // Increment the count for the month
            acc[yearMonth] = (acc[yearMonth] || 0) + 1;
        }
        return acc;
    }, {});

    // Use 'flightCounts' for D3.js visualizations
    console.log(flightCounts);
}).catch(function(error) {
    console.error("Error loading the data:", error);
});
