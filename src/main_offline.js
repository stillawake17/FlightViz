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

// Function to list flights by day, including the time category and flight number
function listFlightsByDay(flightData, year, airportCode) {
    airportCode = airportCode.toLowerCase(); // Ensure airportCode is lowercase
    const flightsByDay = {};
    const seenFlights = new Set();

    flightData.forEach(flight => {
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
                const date = new Date(actualTime);
                if (date.getFullYear() === year) {
                    const dayKey = `${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate()}`;
                    const timeCategory = getTimeCategory(date.getHours(), date.getMinutes());
                    // Include flight number in the output, assuming it's stored in a property like flight.flightNumber
                    const flightNumber = flight.flightNumber || 'Unknown'; // Adjust based on actual data structure

                    if (!flightsByDay[dayKey]) {
                        flightsByDay[dayKey] = [];
                    }
                    flightsByDay[dayKey].push({
                        ...flight, // Includes all original flight data
                        Time_Category: timeCategory,
                        Flight_Number: flightNumber
                    });
                }
            }
        }
    });

    return flightsByDay;
}

// Read and parse the JSON file
const filePath = '../data/combined_strip.json'; // Adjust this path as necessary


fs.readFile(filePath, 'utf8', (err, data) => {
    if (err) {
        console.error('Error reading the file:', err);
        return;
    }
    try {
        const flightData = JSON.parse(data);

        // Process and list flights by day for 2023 and 2024 for 'brs' airport
        const flightsByDay2023 = listFlightsByDay(flightData, 2023, 'brs');
        const flightsByDay2024 = listFlightsByDay(flightData, 2024, 'brs');

        // Write the data for 2023 and 2024 to separate files
        fs.writeFile('./flights_by_day_2023.json', JSON.stringify(flightsByDay2023, null, 2), (err) => {
            if (err) {
                console.error('Error writing data for 2023:', err);
            } else {
                console.log('Flight data for 2023 written to file successfully.');
            }
        });

        fs.writeFile('./flights_by_day_2024.json', JSON.stringify(flightsByDay2024, null, 2), (err) => {
            if (err) {
                console.error('Error writing data for 2024:', err);
            } else {
                console.log('Flight data for 2024 written to file successfully.');
            }
        });

    } catch (parseErr) {
        console.error('Error parsing JSON:', parseErr);
    }
});

// / Function to process and categorize flights for a specific year and airport
function processAndCategorizeFlights(flightData, year, airportCode) {
    return flightData.reduce((acc, d) => {
        let actualTime = null;

        // Check for specific airport code in arrival or departure
        if (d.arrival && d.arrival.iataCode === airportCode && d.arrival.actualTime) {
            actualTime = d.arrival.actualTime;
        } else if (d.departure && d.departure.iataCode === airportCode && d.departure.actualTime) {
            actualTime = d.departure.actualTime;
        }

        if (actualTime) {
            let date = new Date(actualTime);
            if (date.getFullYear() === year) {
                d.latestTime = date;
                d.Hour = date.getHours();
                d.Minute = date.getMinutes();
                d.Time_Category = getTimeCategory(d.Hour, d.Minute);
                acc.push(d);
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
        night: new Array(12).fill(0)
    };

    flightData.forEach(d => {
        let flightMonth = d.latestTime.getMonth();

        monthlyCounts.total[flightMonth]++;
        if (d.Time_Category === "Shoulder hour flights") {
            monthlyCounts.shoulder[flightMonth]++;
        }
        if (d.Time_Category === "Night hour arrivals") {
            monthlyCounts.night[flightMonth]++;
        }
    });

    return monthlyCounts;
}
// Your added back code starts here
// Read and parse the JSON file
// const filePath = '../data/combined_strip.json'; // Adjust filePath if necessary
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
            "2024": aggregatedData2024
        };

        // Convert JSON object to string
        let jsonString = JSON.stringify(jsonData, null, 2); // Pretty formatting

        // Write JSON string to a file
        fs.writeFile('combined_output.json', jsonString, (err) => {
            if (err) throw err;
            console.log('Data for 2023 and 2024 written to file');
        });

    } catch (parseErr) {
        console.error('Error parsing JSON:', parseErr);
    }
});