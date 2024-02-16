const fs = require('fs');

// Helper function to create a unique identifier for a flight record
function createFlightKey(flight) {
    const departureCode = flight.departure ? flight.departure.iataCode.toLowerCase() : ''; // Ensure lowercase for consistency
    const arrivalCode = flight.arrival ? flight.arrival.iataCode.toLowerCase() : ''; // Ensure lowercase for consistency
    const departureTime = flight.departure ? flight.departure.actualTime : '';
    return `${departureCode}-${arrivalCode}-${departureTime}`;
}

// Function to determine the time category of a flight
function getTimeCategory(hour, minute) {
    if (hour === 23 && minute < 30) return "Shoulder hour flights";
    if ((hour === 23 && minute >= 30) || hour < 6) return "Night hour arrivals";
    if (hour === 6) return "Shoulder hour flights";
    return "Regular arrivals";
}

// Function to process and categorize flights for a specific year and airport
function processAndCategorizeFlights(flightData, year, airportCode) {
    airportCode = airportCode.toLowerCase(); // Ensure airportCode is lowercase
    const seenFlights = new Set();
    return flightData.reduce((acc, flight) => {
        const flightKey = createFlightKey(flight);
        if (!seenFlights.has(flightKey)) {
            seenFlights.add(flightKey);
            let actualTime = null;

            if (flight.arrival && flight.arrival.iataCode.toLowerCase() === airportCode && flight.arrival.actualTime) {
                actualTime = flight.arrival.actualTime;
            } else if (flight.departure && flight.departure.iataCode.toLowerCase() === airportCode && flight.departure.actualTime) {
                actualTime = flight.departure.actualTime;
            }

            if (actualTime) {
                let date = new Date(actualTime);
                if (date.getFullYear() === year) {
                    flight.latestTime = date;
                    flight.Hour = date.getHours();
                    flight.Minute = date.getMinutes();
                    flight.Time_Category = getTimeCategory(flight.Hour, flight.Minute);
                    acc.push(flight);
                }
            }
        }
        return acc;
    }, []);
}

// Function to aggregate flight data by month and category
function aggregateDataByMonth(flightData) {
    const monthlyCounts = {
        total: new Array(12).fill(0),
        shoulder: new Array(12).fill(0),
        night: new Array(12).fill(0),
    };

    flightData.forEach(flight => {
        let flightMonth = flight.latestTime.getMonth();

        monthlyCounts.total[flightMonth]++;
        if (flight.Time_Category === "Shoulder hour flights") {
            monthlyCounts.shoulder[flightMonth]++;
        }
        if (flight.Time_Category === "Night hour arrivals") {
            monthlyCounts.night[flightMonth]++;
        }
    });

    return monthlyCounts;
}

// Read and parse the JSON file
const filePath = '../data/combined_strip.json';
fs.readFile(filePath, 'utf8', (err, data) => {
    if (err) {
        console.error('Error reading the file:', err);
        return;
    }
    try {
        const flightData = JSON.parse(data);

        // Process and categorize flights for 2023 and 2024 for 'brs' airport
        const data2023 = processAndCategorizeFlights(flightData, 2023, 'brs');
        const data2024 = processAndCategorizeFlights(flightData, 2024, 'brs');

        // Aggregate data by month for 2023 and 2024
        const aggregatedData2023 = aggregateDataByMonth(data2023);
        const aggregatedData2024 = aggregateDataByMonth(data2024);

        // Create JSON objects for 2023 and 2024
        let jsonData = {
            "2023": aggregatedData2023,
            "2024": aggregatedData2024,
        };

        // Convert JSON object to string with pretty formatting
        let jsonString = JSON.stringify(jsonData, null, 2);

        // Write JSON string to a file
        fs.writeFile('combined_output.json', jsonString, (err) => {
            if (err) throw err;
            console.log('Data for 2023 and 2024 written to file');
        });

    } catch (parseErr) {
        console.error('Error parsing JSON:', parseErr);
    }
});
