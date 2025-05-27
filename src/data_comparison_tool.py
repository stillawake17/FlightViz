import json
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from typing import Dict, List, Any, Tuple
import os

class FlightDataComparator:
    """
    Compare flight data from OpenSky Network and AviationStack APIs
    """
    
    def __init__(self):
        self.opensky_data = None
        self.aviationstack_data = None
        
    def load_opensky_data(self, filepath: str) -> Dict[str, Any]:
        """Load OpenSky data from JSON file"""
        try:
            with open(filepath, 'r') as f:
                self.opensky_data = json.load(f)
            print(f"Loaded OpenSky data from {filepath}")
            return self.opensky_data
        except Exception as e:
            print(f"Error loading OpenSky data: {e}")
            return {}
    
    def load_aviationstack_data(self, filepath: str) -> Dict[str, Any]:
        """Load AviationStack data from JSON file"""
        try:
            with open(filepath, 'r') as f:
                self.aviationstack_data = json.load(f)
            print(f"Loaded AviationStack data from {filepath}")
            return self.aviationstack_data
        except Exception as e:
            print(f"Error loading AviationStack data: {e}")
            return {}
    
    def extract_callsigns_opensky(self) -> Tuple[List[str], List[str]]:
        """Extract arrival and departure callsigns from OpenSky data"""
        arrival_callsigns = []
        departure_callsigns = []
        
        if self.opensky_data:
            for flight in self.opensky_data.get('arrivals', []):
                callsign = flight.get('callsign', '').strip()
                if callsign:
                    arrival_callsigns.append(callsign)
            
            for flight in self.opensky_data.get('departures', []):
                callsign = flight.get('callsign', '').strip()
                if callsign:
                    departure_callsigns.append(callsign)
        
        return arrival_callsigns, departure_callsigns
    
    def extract_flight_numbers_aviationstack(self) -> Tuple[List[str], List[str]]:
        """Extract arrival and departure flight numbers from AviationStack data"""
        arrival_flights = []
        departure_flights = []
        
        if self.aviationstack_data:
            for flight in self.aviationstack_data.get('arrivals', []):
                flight_info = flight.get('flight', {})
                flight_number = flight_info.get('iata', '') or flight_info.get('number', '')
                if flight_number:
                    arrival_flights.append(flight_number)
            
            for flight in self.aviationstack_data.get('departures', []):
                flight_info = flight.get('flight', {})
                flight_number = flight_info.get('iata', '') or flight_info.get('number', '')
                if flight_number:
                    departure_flights.append(flight_number)
        
        return arrival_flights, departure_flights
    
    def analyze_coverage_overlap(self) -> Dict[str, Any]:
        """Analyze overlap between OpenSky and AviationStack data"""
        if not self.opensky_data or not self.aviationstack_data:
            return {"error": "Both datasets must be loaded"}
        
        # Get flight identifiers
        opensky_arrivals, opensky_departures = self.extract_callsigns_opensky()
        aviationstack_arrivals, aviationstack_departures = self.extract_flight_numbers_aviationstack()
        
        # Convert to sets for comparison
        opensky_arr_set = set(opensky_arrivals)
        opensky_dep_set = set(opensky_departures)
        aviation_arr_set = set(aviationstack_arrivals)
        aviation_dep_set = set(aviationstack_departures)
        
        # Find overlaps and differences
        analysis = {
            'arrivals': {
                'opensky_only': len(opensky_arr_set - aviation_arr_set),
                'aviationstack_only': len(aviation_arr_set - opensky_arr_set),
                'common': len(opensky_arr_set & aviation_arr_set),
                'opensky_total': len(opensky_arr_set),
                'aviationstack_total': len(aviation_arr_set),
                'opensky_unique': list(opensky_arr_set - aviation_arr_set)[:10],  # First 10
                'aviationstack_unique': list(aviation_arr_set - opensky_arr_set)[:10]
            },
            'departures': {
                'opensky_only': len(opensky_dep_set - aviation_dep_set),
                'aviationstack_only': len(aviation_dep_set - opensky_dep_set),
                'common': len(opensky_dep_set & aviation_dep_set),
                'opensky_total': len(opensky_dep_set),
                'aviationstack_total': len(aviation_dep_set),
                'opensky_unique': list(opensky_dep_set - aviation_dep_set)[:10],
                'aviationstack_unique': list(aviation_dep_set - opensky_dep_set)[:10]
            }
        }
        
        return analysis
    
    def generate_comparison_report(self) -> str:
        """Generate detailed comparison report"""
        if not self.opensky_data or not self.aviationstack_data:
            return "Error: Both datasets must be loaded"
        
        date = self.aviationstack_data.get('date', 'Unknown')
        
        report = f"""
=== FLIGHT DATA COMPARISON REPORT ===
Date: {date}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

DATA SOURCES:
- OpenSky Network: Real-time aircraft tracking
- AviationStack: Commercial flight schedules

BASIC STATISTICS:
"""
        
        # Basic counts
        opensky_summary = self.opensky_data.get('summary', {})
        aviationstack_summary = self.aviationstack_data.get('summary', {})
        
        report += f"""
OpenSky Network:
  Total Flights: {opensky_summary.get('total_flights', 0)}
  Arrivals: {opensky_summary.get('arrivals_count', 0)}
  Departures: {opensky_summary.get('departures_count', 0)}

AviationStack:
  Total Flights: {aviationstack_summary.get('total_flights', 0)}
  Arrivals: {aviationstack_summary.get('arrivals', 0)}
  Departures: {aviationstack_summary.get('departures', 0)}
"""
        
        # Coverage analysis
        coverage = self.analyze_coverage_overlap()
        
        if 'error' not in coverage:
            report += f"""
COVERAGE ANALYSIS:

Arrivals:
  OpenSky Only: {coverage['arrivals']['opensky_only']}
  AviationStack Only: {coverage['arrivals']['aviationstack_only']}
  Common Flights: {coverage['arrivals']['common']}
  
  Coverage Rate:
    OpenSky captures {coverage['arrivals']['common']}/{coverage['arrivals']['aviationstack_total']} ({(coverage['arrivals']['common']/max(coverage['arrivals']['aviationstack_total'], 1)*100):.1f}%) of AviationStack flights
    AviationStack captures {coverage['arrivals']['common']}/{coverage['arrivals']['opensky_total']} ({(coverage['arrivals']['common']/max(coverage['arrivals']['opensky_total'], 1)*100):.1f}%) of OpenSky flights

Departures:
  OpenSky Only: {coverage['departures']['opensky_only']}
  AviationStack Only: {coverage['departures']['aviationstack_only']}
  Common Flights: {coverage['departures']['common']}
  
  Coverage Rate:
    OpenSky captures {coverage['departures']['common']}/{coverage['departures']['aviationstack_total']} ({(coverage['departures']['common']/max(coverage['departures']['aviationstack_total'], 1)*100):.1f}%) of AviationStack flights
    AviationStack captures {coverage['departures']['common']}/{coverage['departures']['opensky_total']} ({(coverage['departures']['common']/max(coverage['departures']['opensky_total'], 1)*100):.1f}%) of OpenSky flights

UNIQUE TO OPENSKY (Arrivals - first 10):
{chr(10).join('  - ' + flight for flight in coverage['arrivals']['opensky_unique']) if coverage['arrivals']['opensky_unique'] else '  None'}

UNIQUE TO AVIATIONSTACK (Arrivals - first 10):
{chr(10).join('  - ' + flight for flight in coverage['arrivals']['aviationstack_unique']) if coverage['arrivals']['aviationstack_unique'] else '  None'}
"""
        
        # Data quality analysis
        report += self._analyze_data_quality()
        
        # Recommendations
        report += """
RECOMMENDATIONS:

1. DATA COMPLETENESS:
   - AviationStack provides more complete commercial flight schedules
   - OpenSky captures more general aviation and military traffic
   - Use AviationStack for commercial flight analysis
   - Use OpenSky for comprehensive air traffic monitoring

2. DATA ACCURACY:
   - AviationStack has structured, scheduled flight data
   - OpenSky provides real-time, actual flight tracking
   - OpenSky times are more accurate for actual operations
   - AviationStack better for planning and schedule analysis

3. USE CASES:
   - Compliance monitoring: Combine both sources
   - Schedule analysis: Primarily AviationStack
   - Traffic monitoring: Primarily OpenSky
   - Night flight enforcement: Use both for comprehensive coverage
"""
        
        return report
    
    def _analyze_data_quality(self) -> str:
        """Analyze data quality aspects"""
        quality_report = "\nDATA QUALITY ANALYSIS:\n"
        
        # Analyze AviationStack data quality
        if self.aviationstack_data:
            aviationstack_flights = (
                self.aviationstack_data.get('arrivals', []) + 
                self.aviationstack_data.get('departures', [])
            )
            
            time_categories = {}
            airlines = set()
            
            for flight in aviationstack_flights:
                # Time categories
                category = flight.get('time_category', 'Unknown')
                time_categories[category] = time_categories.get(category, 0) + 1
                
                # Airlines
                airline = flight.get('airline', {}).get('name', '')
                if airline:
                    airlines.add(airline)
            
            quality_report += f"""
AviationStack Data Quality:
  Time Categories: {dict(time_categories)}
  Unique Airlines: {len(airlines)}
  Sample Airlines: {', '.join(list(airlines)[:5])}
"""
        
        # Analyze OpenSky data quality
        if self.opensky_data:
            opensky_flights = (
                self.opensky_data.get('arrivals', []) + 
                self.opensky_data.get('departures', [])
            )
            
            with_callsign = sum(1 for f in opensky_flights if f.get('callsign', '').strip())
            with_position = sum(1 for f in opensky_flights if f.get('latitude') and f.get('longitude'))
            on_ground = sum(1 for f in opensky_flights if f.get('on_ground'))
            
            quality_report += f"""
OpenSky Data Quality:
  Flights with Callsign: {with_callsign}/{len(opensky_flights)} ({(with_callsign/max(len(opensky_flights), 1)*100):.1f}%)
  Flights with Position: {with_position}/{len(opensky_flights)} ({(with_position/max(len(opensky_flights), 1)*100):.1f}%)
  Aircraft on Ground: {on_ground}/{len(opensky_flights)} ({(on_ground/max(len(opensky_flights), 1)*100):.1f}%)
"""
        
        return quality_report
    
    def create_visual_comparison(self, save_path: str = None