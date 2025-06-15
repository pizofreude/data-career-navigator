"""
File: src/extractors/location_extractor.py
----------------------------------------------
ETL pipeline to extract country from job postings based on location strings.
"""

# Import necessary libraries
import re
from country_converter import CountryConverter
import pycountry
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# Initialize CountryConverter and geolocator with rate limiting
cc = CountryConverter()
geolocator = Nominatim(user_agent="data-career-navigator-country-extractor")
geocode = RateLimiter(
    geolocator.geocode,
    min_delay_seconds=2,
    error_wait_seconds=5.0,
    max_retries=2,
    swallow_exceptions=True
)

# US state abbreviations
US_STATE_ABBR = {
    'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy', 'dc'
}

# Major city/region to country mapping for common cases
CITY_TO_COUNTRY = {
    'new york': 'United States',
    'san francisco': 'United States',
    'san francisco bay': 'United States',
    'bay area': 'United States',
    'washington dc': 'United States',
    'washington dc-baltimore': 'United States',
    'dallas-fort worth': 'United States',
    'utica-rome': 'United States',
    'columbus, ohio': 'United States',
    'austin, texas': 'United States',
    'greater chicago': 'United States',
    'greater st. louis': 'United States',
    'greater tampa bay': 'United States',
    'greater minneapolis-st. paul': 'United States',
    'salt lake': 'United States',
    'charlotte': 'United States',
    'kansas': 'United States',
    'des moines': 'United States',
    'greater paris': 'France',
    'paris': 'France',
    'porto': 'Portugal',
    'lisbon': 'Portugal',
    'mumbai': 'India',
    'greater kolkata': 'India',
    'bangkok': 'Thailand',
    'jakarta': 'Indonesia',
    'bengaluru': 'India',
    'greater bengaluru': 'India',
    'greater istanbul': 'Turkey',
    'greater coventry': 'United Kingdom',
    'hongkou': 'China',
    'kuala lumpur': 'Malaysia',
    'mexico city': 'Mexico',
    'mexico': 'Mexico',
    'rio de janeiro': 'Brazil',
    'greater rio de janeiro': 'Brazil',
    'campinas': 'Brazil',
    'brazil': 'Brazil',
    'calgary': 'Canada',
    'canada': 'Canada',
    'istanbul': 'Turkey',
    'chicago': 'United States',
    'st. louis': 'United States',
    'tampa': 'United States',
    'rome': 'Italy',
    'coventry': 'United Kingdom',
    'kolkata': 'India',
    'utah': 'United States',
    'dallas': 'United States',
    'fort worth': 'United States',
    'baltimore': 'United States',
    'minneapolis': 'United States',
    'st. paul': 'United States',
    'bay area': 'United States',
    'porto': 'Portugal',
    'nasr': 'Egypt',
    # Add more as needed
}

# Example: Expand CITY_TO_COUNTRY mapping for more coverage
CITY_TO_COUNTRY.update({
    'los angeles': 'United States',
    'boston': 'United States',
    'seattle': 'United States',
    'toronto': 'Canada',
    'vancouver': 'Canada',
    'delhi': 'India',
    'beijing': 'China',
    'shanghai': 'China',
    'são paulo': 'Brazil',
    'madrid': 'Spain',
    'barcelona': 'Spain',
    'berlin': 'Germany',
    'munich': 'Germany',
    'london': 'United Kingdom',
    'manchester': 'United Kingdom',
    'sydney': 'Australia',
    'melbourne': 'Australia',
    # Add more as needed
})

# Simple in-memory cache for geopy lookups
_geocode_cache = {}

def extract_country(location_str):
    if not location_str or not isinstance(location_str, str):
        print(f"[extract_country] Empty or invalid location: {location_str}")
        return 'Unknown'
    loc = location_str.strip().lower()
    print(f"[extract_country] Processing: '{location_str}' -> '{loc}'")
    # Remove common suffixes like 'metropolitan area', 'metroplex', 'metro', 'region', 'area', 'county', 'greater'
    loc = re.sub(r'\b(metropolitan area|metroplex|metro|region|area|county|greater)\b', '', loc, flags=re.IGNORECASE)
    loc = re.sub(r'\s+', ' ', loc).strip(' ,')
    # Try city/region mapping first
    for city in CITY_TO_COUNTRY:
        if city in loc:
            print(f"[extract_country] Matched city/region mapping: '{city}' -> '{CITY_TO_COUNTRY[city]}'")
            return CITY_TO_COUNTRY[city]
    # Try cached geopy lookup
    if loc in _geocode_cache:
        print(f"[extract_country] Cache hit for '{loc}': {_geocode_cache[loc]}")
        return _geocode_cache[loc]
    # Try geopy first for any city/state/country string
    try:
        geo = geocode(loc, addressdetails=True, language='en', timeout=10)
        if geo and geo.raw and 'address' in geo.raw:
            address = geo.raw['address']
            if 'country' in address:
                print(f"[extract_country] Geopy found: '{loc}' -> '{address['country']}'")
                _geocode_cache[loc] = address['country']
                return address['country']
    except Exception as e:
        print(f"[extract_country] Geopy error for '{loc}': {e}")
    _geocode_cache[loc] = 'Unknown'
    # Try last part (e.g., state or country)
    parts = [p.strip() for p in re.split(r',', loc)]
    if parts:
        last = parts[-1]
        country = cc.convert(names=last, to='name_short', not_found=None)
        if country and country != 'not found':
            print(f"[extract_country] CountryConverter last part: '{last}' -> '{country}'")
            return country
    # Try direct country match for the whole string
    country = cc.convert(names=loc, to='name_short', not_found=None)
    if country and country != 'not found':
        print(f"[extract_country] CountryConverter full: '{loc}' -> '{country}'")
        return country
    # Try fuzzy matching with pycountry
    for country_obj in pycountry.countries:
        if country_obj.name.lower() in loc:
            print(f"[extract_country] PyCountry fuzzy: '{loc}' -> '{country_obj.name}'")
            return country_obj.name
        if hasattr(country_obj, 'official_name') and country_obj.official_name.lower() in loc:
            print(f"[extract_country] PyCountry fuzzy official: '{loc}' -> '{country_obj.name}'")
            return country_obj.name
    print(f"[extract_country] No match for '{loc}', returning 'Unknown'")
    return 'Unknown'