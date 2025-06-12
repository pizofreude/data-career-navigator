# -*- coding: utf-8 -*-
# src\salary_extractor.py

"""
File: salary_extractor.py
--------------------------
Enhanced Salary Extractor and ETL Pipeline.
This module provides a comprehensive salary extraction tool that can handle various
currency formats, salary ranges, and different salary periods.
It includes an ETL pipeline to process job postings and
automatically extract salary information into a structured format.

Usage: You can run this module directly.
Note: The test_salary_extractor() function verifies a wide range of salary formats,
and demo_etl_pipeline() shows how to apply SalaryETL to a sample DataFrame for production use.
"""

# Import necessary libraries
import time
import os
import json
import re
import math
from typing import List
import pandas as pd
from geopy.geocoders import Nominatim
from countryinfo import CountryInfo

class SalaryExtractor:
    """
    Extractor for salary information from job postings.
    This class uses regular expressions to identify and extract salary details.
    """
    def __init__(self):
        # Major currency symbols and codes
        self.currency_symbols = {
            'MX$',
            # Symbols
            '$', '£', '€', '¥', '₹', '₽', '₩', '₪', '₦', '₡', '₴', '₨', '₵', '₫', '₮', '₯', '₰', '₱', '₲', '₳', '₴', '₵', '₶', '₷', '₸', '₹', '₺', '₻', '₼', '₽', '₾', '₿', '＄', '￠', '￡', '￢', '￣', '￤', '￥', '￦',
            # Major currency codes with variations
            'USD', 'EUR', 'GBP', 'JPY', 'CNY', 'INR', 'CAD', 'AUD', 'CHF', 'SEK', 'NOK', 'DKK', 'RUB', 'KRW', 'SGD', 'HKD', 'NZD', 'MXN', 'BRL', 'ZAR', 'THB', 'MYR', 'IDR', 'PHP', 'VND', 'TWD', 'PLN', 'CZK', 'HUF', 'TRY', 'ILS', 'AED', 'SAR', 'EGP', 'QAR', 'KWD', 'BHD', 'OMR', 'JOD', 'LBP', 'PKR', 'LKR', 'BDT', 'NPR', 'AFN', 'MMK', 'LAK', 'KHR', 'BND', 'FJD', 'PGK', 'SBD', 'TOP', 'VUV', 'WST',
            # Regional names and abbreviations
            'RM', 'MYR', 'RINGGIT', 'MALAYSIAN RINGGIT',
            'SGD', 'SINGAPORE DOLLAR', 'S$',
            'RUPIAH', 'IDR', 'RP',
            'BAHT', 'THB', '฿',
            'PESO', 'PESOS', 'PHP', '₱',
            'DONG', 'VND', '₫',
            'RUPEE', 'RUPEES', 'INR', 'RS', '₹',
            'YUAN', 'RENMINBI', 'CNY', 'RMB', '¥',
            'WON', 'KRW', '₩',
            'DIRHAM', 'AED', 'DH',
            'RIYAL', 'SAR', 'SR',
            'SHEKEL', 'ILS', '₪',
            'LIRA', 'TRY', '₺',
            'RUBLE', 'ROUBLE', 'RUB', '₽',
            'RAND', 'ZAR', 'R',
            'REAL', 'BRL', 'R$',
            'KRONA', 'KRONOR', 'SEK', 'KR',
            'KRONE', 'KRONER', 'NOK', 'DKK',
            'FRANC', 'CHF', 'FR',
            'ZLOTY', 'PLN', 'ZŁ',
            'FORINT', 'HUF', 'FT',
            'KORUNA', 'CZK', 'KČ',
            'DINAR', 'KWD', 'BHD', 'JOD', 'DZD', 'IQD', 'LYD', 'TND',
            'NAIRA', 'NGN', '₦',
            'CEDI', 'GHS', '₵',
            'BIRR', 'ETB',
            'SHILLING', 'KES', 'UGX', 'TZS',
            'AFGHANI', 'AFN', '؋',
            'TAKA', 'BDT', '৳',
            'KYAT', 'MMK', 'K',
            'KIP', 'LAK', '₭',
            'RIEL', 'KHR', '៛',
        }

        # Common salary period indicators
        self.period_indicators = [
            'per year', 'annually', 'yearly', 'per annum', 'p.a.', 'pa',
            'per month', 'monthly', 'per mth', 'p.m.', 'pm',
            'per hour', 'hourly', 'per hr', 'p.h.', 'ph',
            'per week', 'weekly', 'per wk', 'p.w.', 'pw',
            'per day', 'daily', 'per diem'
        ]

        # Build the comprehensive regex pattern
        self._build_pattern()

    def _build_pattern(self):
        # Create currency pattern (case-insensitive)
        currency_list = sorted(list(self.currency_symbols), key=len, reverse=True)
        currency_pattern = '|'.join(re.escape(curr) for curr in currency_list)

        # Period indicators pattern
        period_pattern = '|'.join(re.escape(period) for period in self.period_indicators)

        # Number patterns for different formats
        # Improved: match full numbers with thousands separators, e.g. 235,200 or 252,806
        number_pattern = r'(?:\d{1,3}(?:[.,]\d{3})+|\d+)(?:\.\d+)?[kKmM]?'

        # Currency symbol/code pattern (allow NO whitespace between currency and number)
        currency_prefix = rf'(?:({currency_pattern}))'
        currency_suffix = rf'(?:({currency_pattern}))'

        # Full regex patterns for salary extraction
        self.patterns = [
            # Pattern 1: Currency prefix required for both numbers (e.g., MX$235,200- MX$252,806)
            rf'{currency_prefix}({number_pattern})\s*[-–—to]+\s*{currency_prefix}({number_pattern})(?:\s*({period_pattern}))?',

            # Pattern 1.5: Currency prefix before first number, then range, then second number (no currency on second)
            rf'{currency_prefix}({number_pattern})\s*[-–—to]+\s*({number_pattern})(?:\s*({period_pattern}))?',

            # Pattern 2: Currency suffix with range (e.g., 50,000-80,000 USD)
            rf'({number_pattern})\s*[-–—to]?\s*({number_pattern})\s*{currency_suffix}(?:\s*({period_pattern}))?',

            # Pattern 3: Single salary with currency prefix (e.g., $75,000 annually)
            rf'{currency_prefix}({number_pattern})(?:\s*({period_pattern}))?',

            # Pattern 4: Single salary with currency suffix (e.g., 75,000 USD per year)
            rf'({number_pattern})\s*{currency_suffix}(?:\s*({period_pattern}))?',

            # Pattern 5: Salary range without explicit currency in between
            rf'({number_pattern})\s*[-–—to]\s*({number_pattern})\s*{currency_suffix}(?:\s*({period_pattern}))?',

            # Pattern 6: Complex patterns like "Salary: MYR 5,000 - 8,000"
            rf'(?:salary|compensation|pay|wage|income)[:]\s*{currency_prefix}?({number_pattern})\s*[-–—to]\s*{currency_prefix}?({number_pattern})(?:\s*({period_pattern}))?',
        ]

        # Debug: print the actual regex for Pattern 1 and the currency alternation
        print("[DEBUG] Pattern 1 regex:", self.patterns[0] if hasattr(self, 'patterns') else 'not built yet')
        print("[DEBUG] Pattern 1.5 regex:", self.patterns[1] if hasattr(self, 'patterns') else 'not built yet')
        print("[DEBUG] Currency alternation:", currency_pattern)

        # Compile all patterns (case-insensitive)
        self.compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.patterns]

    def extract_salaries(self, text: str) -> List[dict]:
        """
        Extracts salary information from the given text using predefined regex patterns.
        Only considers sentences with salary-related keywords and ignores funding/investment contexts.
        Filters out implausible salary values (zero, negative, or below a minimum threshold).
        """
        
        results = []

        salary_keywords = [
            "salary", "compensation", "pay", "base pay", "base salary", "annual", "per year", "per annum",
            "yearly", "monthly", "per month", "hourly", "per hour", "per week", "per day", "per diem",
            "remuneration", "wage", "package", "rate", "earn", "income"
        ]
        funding_keywords = [
            "funding", "raised", "investment", "series a", "series b", "series c", "venture", "capital",
            "backed", "round", "financing", "investor", "acrew", "sequoia", "bain", "homebrew", "visa", "million", "billion"
        ]

        # Minimum plausible salary (annualized, in any currency, before conversion)
        MIN_REASONABLE_SALARY = 5000  # e.g., $5,000/year or equivalent

        # Split text into sentences (more granular than lines/paragraphs)
        sentences = re.split(r'(?<=[.!?])\s+', text)
        candidate_sentences = [
            sent for sent in sentences
            if any(kw in sent.lower() for kw in salary_keywords)
            and not any(fk in sent.lower() for fk in funding_keywords)
        ]
        # If no candidate sentences, fallback to all sentences
        if not candidate_sentences:
            candidate_sentences = sentences

        # Run extraction only on candidate sentences
        for i, pattern in enumerate(self.compiled_patterns):
            for sent in candidate_sentences:
                matches = pattern.finditer(sent)
                for match in matches:
                    groups = match.groups()
                    salary_info = {
                        'pattern_used': i + 1,
                        'full_match': match.group(0).strip(),
                        'currency': None,
                        'min_salary': None,
                        'max_salary': None,
                        'single_salary': None,
                        'period': None,
                        'position': (match.start(), match.end())
                    }


                    try:
                        if 'MX$' in match.group(0) or 'mx$' in match.group(0):
                            print(f"[DEBUG] Pattern {i+1} match: {match.group(0)}")
                            print(f"[DEBUG] Groups: {groups}")
                        if i == 0:  # Pattern 1: currency prefix with range
                            currency1, min_sal, currency2, max_sal, period = (groups + (None,)*5)[:5]
                            currency = currency1 or currency2
                            salary_info['currency'] = self._normalize_currency(currency)
                            salary_info['min_salary'] = self._normalize_number(min_sal)
                            salary_info['max_salary'] = self._normalize_number(max_sal)
                            salary_info['period'] = period.lower().strip() if period else None

                        elif i == 1:  # Pattern 1.5: currency prefix before first number, then range, then second number (no currency)
                            currency, min_sal, max_sal, period = (groups + (None,)*4)[:4]
                            salary_info['currency'] = self._normalize_currency(currency)
                            salary_info['min_salary'] = self._normalize_number(min_sal)
                            salary_info['max_salary'] = self._normalize_number(max_sal)
                            salary_info['period'] = period.lower().strip() if period else None

                        elif i == 2:  # Pattern 2: currency suffix with range
                            min_sal, max_sal, currency, period = (groups + (None,)*4)[:4]
                            salary_info['currency'] = self._normalize_currency(currency)
                            salary_info['min_salary'] = self._normalize_number(min_sal)
                            salary_info['max_salary'] = self._normalize_number(max_sal)
                            salary_info['period'] = period.lower().strip() if period else None

                        elif i == 3:  # Pattern 3: single salary with currency prefix
                            currency, single_sal, period = (groups + (None,)*3)[:3]
                            salary_info['currency'] = self._normalize_currency(currency)
                            salary_info['single_salary'] = self._normalize_number(single_sal)
                            salary_info['period'] = period.lower().strip() if period else None

                        elif i == 4:  # Pattern 4: single salary with currency suffix
                            single_sal, currency, period = (groups + (None,)*3)[:3]
                            salary_info['currency'] = self._normalize_currency(currency)
                            salary_info['single_salary'] = self._normalize_number(single_sal)
                            salary_info['period'] = period.lower().strip() if period else None

                        elif i == 5:  # Pattern 5: salary range without explicit currency in between
                            min_sal, max_sal, currency, period = (groups + (None,)*4)[:4]
                            salary_info['currency'] = self._normalize_currency(currency)
                            salary_info['min_salary'] = self._normalize_number(min_sal)
                            salary_info['max_salary'] = self._normalize_number(max_sal)
                            salary_info['period'] = period.lower().strip() if period else None

                        elif i == 6:  # Pattern 6: complex patterns like "Salary: MYR 5,000 - 8,000"
                            currency1, min_sal, currency2, max_sal, period = (groups + (None,)*5)[:5]
                            currency = currency1 or currency2
                            salary_info['currency'] = self._normalize_currency(currency)
                            salary_info['min_salary'] = self._normalize_number(min_sal)
                            salary_info['max_salary'] = self._normalize_number(max_sal)
                            salary_info['period'] = period.lower().strip() if period else None

                        # --- Guardrails for implausible values ---
                        # Swap min/max if needed
                        if (
                            salary_info['min_salary'] is not None and salary_info['max_salary'] is not None and
                            salary_info['min_salary'] > salary_info['max_salary']
                        ):
                            salary_info['min_salary'], salary_info['max_salary'] = salary_info['max_salary'], salary_info['min_salary']

                        # Filter out zero, negative, or implausibly low values
                        for key in ['min_salary', 'max_salary', 'single_salary']:
                            val = salary_info[key]
                            if val is not None:
                                if (not isinstance(val, (int, float)) or math.isnan(val) or val < MIN_REASONABLE_SALARY):
                                    salary_info[key] = None

                        # Only append if we have a currency and at least one plausible salary value
                        if salary_info['currency'] and (
                            salary_info['min_salary'] is not None or
                            salary_info['max_salary'] is not None or
                            salary_info['single_salary'] is not None
                        ):
                            results.append(salary_info)

                    except (ValueError, TypeError, AttributeError, IndexError) as e:
                        print(f"[Warning] Pattern {i+1} failed to parse: {e}")

        # Always return a list, even if empty
        return self._deduplicate_results(results)
    def _normalize_currency(self, currency):
        if not currency:
            return None
        c = currency.upper().replace(' ', '')
        # Map common currency symbols/codes to ISO codes
        mapping = {
            'MX$': 'MXN', 'MXN': 'MXN', '$': 'USD', 'USD': 'USD', 'US$': 'USD',
            '€': 'EUR', 'EUR': 'EUR', '£': 'GBP', 'GBP': 'GBP', '¥': 'JPY', 'JPY': 'JPY',
            'C$': 'CAD', 'CAD': 'CAD', 'A$': 'AUD', 'AUD': 'AUD',
            'MYR': 'MYR', 'RM': 'MYR', 'SGD': 'SGD', 'S$': 'SGD',
            'IDR': 'IDR', 'RP': 'IDR', '₱': 'PHP', 'PHP': 'PHP',
            'VND': 'VND', '₫': 'VND', 'INR': 'INR', '₹': 'INR',
            'CNY': 'CNY', 'RMB': 'CNY', 'KRW': 'KRW', '₩': 'KRW',
            'BRL': 'BRL', 'R$': 'BRL', 'ZAR': 'ZAR', 'R': 'ZAR',
            'THB': 'THB', '฿': 'THB', 'PLN': 'PLN', 'CZK': 'CZK', 'HUF': 'HUF',
            'TRY': 'TRY', '₺': 'TRY', 'ILS': 'ILS', '₪': 'ILS',
        }
        # Try to match the mapping, else return the cleaned code
        return mapping.get(c, c)


    def _is_number(self, text: str) -> bool:
        """Check if text represents a salary number"""
        if not text:
            return False
        # Remove common separators and check if it contains digits
        cleaned = re.sub(r'[.,kKmM\s]', '', text)
        return bool(re.search(r'\d', cleaned))

    def _normalize_number(self, number_str: str) -> float:
        """Convert number string to float value"""
        if not number_str:
            return 0.0

        original_str = number_str.strip()

        # Handle M/m multiplier (million)
        if original_str.lower().endswith('m'):
            multiplier = 1_000_000
            number_str = re.sub(r'[mM]$', '', number_str)
        # Handle K/k multiplier (thousand)
        elif original_str.lower().endswith('k'):
            multiplier = 1_000
            number_str = re.sub(r'[kK]$', '', number_str)
        else:
            multiplier = 1

        # Handle European vs US number formatting
        # European: 50.000,50  (period=thousands, comma=decimal)
        # US: 50,000.50      (comma=thousands, period=decimal)
        if ',' in number_str and '.' in number_str:
            last_comma = number_str.rfind(',')
            last_period = number_str.rfind('.')
            if last_period > last_comma:
                # US format: remove commas
                number_str = number_str.replace(',', '')
            else:
                # European format: remove periods and replace comma with period
                number_str = number_str.replace('.', '').replace(',', '.')
        else:
            # Only one separator type
            if '.' in number_str and ',' not in number_str:
                # Might be European thousands, e.g. "4.500.000"
                if number_str.count('.') > 1:
                    number_str = number_str.replace('.', '')
                # else single period likely decimal, keep it
            # Remove commas and spaces
            number_str = re.sub(r'[,\s]', '', number_str)

        try:
            return float(number_str) * multiplier
        except ValueError:
            return 0.0

    def _deduplicate_results(self, results: List[dict]) -> List[dict]:
        """Remove duplicate and overlapping salary extractions"""
        if not results:
            return results

        # Sort by position
        results.sort(key=lambda x: x['position'][0])
        filtered_results = [results[0]]

        for result in results[1:]:
            last_result = filtered_results[-1]
            # If positions don't overlap significantly, add the result
            if result['position'][0] >= last_result['position'][1] - 5:
                filtered_results.append(result)
            # If current result is more complete, replace the last one
            elif (result['min_salary'] is not None and result['max_salary'] is not None and
                  not (last_result['min_salary'] is not None and last_result['max_salary'] is not None)):
                filtered_results[-1] = result

        return filtered_results


class SalaryETL:
    """ETL pipeline for processing salary data from job posts"""

    def __init__(self):
        self.extractor = SalaryExtractor()
        self._exchange_rates_cache = None
        self._exchange_rates_cache_time = 0
        self._cache_ttl = 60 * 60  # 1 hour

    def get_exchange_rates_to_usd(self):
        """
        Load exchange rates to USD from a local JSON file (exchange_rates_usd.json).
        This file should be updated daily by a GitHub Action.
        """
        now = time.time()
        if (
            self._exchange_rates_cache is not None
            and now - self._exchange_rates_cache_time < self._cache_ttl
        ):
            return self._exchange_rates_cache
    
        json_path = os.path.join(os.path.dirname(__file__), "exchange_rates_usd.json")
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                rates = json.load(f)
            # No filtering: use all available rates
            rates['USD'] = 1.0
            self._exchange_rates_cache = rates
            self._exchange_rates_cache_time = now
            return rates
        except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
            print(f"Warning: Could not load exchange rates from exchange_rates_usd.json: {e}")
    
        # Fallback to static rates (optional: you can expand this to all 161 if you want)
        return {'USD': 1.0}

    def process_job_dataframe(self, df, text_column='description', include_title=True, title_column='title'):
        """
        Process a DataFrame of job posts to extract salary information

        Args:
            df: DataFrame with job posts
            text_column: Column containing job description text (default: 'description')
            include_title: Whether to include job title in salary search
            title_column: Column containing job titles (default: 'title')

        Returns:
            DataFrame with added salary columns
        """
        df = df.copy()

        # --- Normalize text fields for extraction ---
        if title_column in df.columns:
            df[title_column] = df[title_column].fillna('').astype(str).str.strip()
        if text_column in df.columns:
            df[text_column] = df[text_column].fillna('').astype(str).str.strip()

        salary_columns = [
            'has_salary', 'currency_raw', 'min_salary_raw', 'max_salary_raw',
            'single_salary_raw', 'salary_period', 'min_salary_annual_usd',
            'max_salary_annual_usd', 'avg_salary_annual_usd', 'salary_confidence'
        ]
        for col in salary_columns:
            df[col] = None

        exchange_rates = self.get_exchange_rates_to_usd()
        MAX_REASONABLE_SALARY = 1_000_000  # adjust as needed

        # --- Add MXN to currency_map and iso_codes ---
        currency_map = {
            '$': 'USD', 'usd': 'USD', 'us$': 'USD', 's$': 'SGD', '£': 'GBP', 'gbp': 'GBP', '€': 'EUR', 'eur': 'EUR',
            'rs': 'INR', '₹': 'INR', 'inr': 'INR',
            'rm': 'MYR', 'myr': 'MYR',
            'sgd': 'SGD',
            'idr': 'IDR', 'rp': 'IDR',
            'thb': 'THB', '฿': 'THB',
            'php': 'PHP', '₱': 'PHP',
            'vnd': 'VND', '₫': 'VND',
            'r': 'ZAR', 'zar': 'ZAR',
            'top': 'TOP',
            # Robust MXN mapping
            'mx$': 'MXN', 'mxn': 'MXN', 'mx': 'MXN', 'mx pesos': 'MXN', 'mexican peso': 'MXN', 'mexican pesos': 'MXN',
            # Add more variants for robustness
            'k': None, 'm': None, '': None, None: None
        }
        iso_codes = {'USD','MYR','SGD','EUR','GBP','INR','THB','IDR','PHP','VND','ZAR','TOP','MXN'}

        # Track which rows have a valid extracted salary+currency
        extracted_currency_mask = []

        for idx, row in df.iterrows():
            # Try header_text first if available
            header_text = row.get('header_text', None)
            header_salary_results = []
            if header_text and isinstance(header_text, str) and header_text.strip():
                header_salary_results = self.extractor.extract_salaries(header_text)
                # Filter out implausible values
                filtered_header_results = []
                for r in header_salary_results:
                    valid = True
                    for k in ['single_salary', 'min_salary', 'max_salary']:
                        v = r.get(k)
                        if v is not None and v > MAX_REASONABLE_SALARY:
                            valid = False
                    if valid:
                        filtered_header_results.append(r)
                header_salary_results = filtered_header_results

            if header_salary_results:
                best_result = self._select_best_salary_result(header_salary_results)
            else:
                # Fallback to title+description logic
                text_to_search = ''
                if pd.notna(row.get(text_column, '')):
                    text_to_search = row[text_column]
                if include_title and title_column in df.columns and pd.notna(row.get(title_column, '')):
                    text_to_search = f"{row[title_column]} {text_to_search}"

                salary_results = self.extractor.extract_salaries(text_to_search)
                filtered_results = []
                for r in salary_results:
                    valid = True
                    for k in ['single_salary', 'min_salary', 'max_salary']:
                        v = r.get(k)
                        if v is not None and v > MAX_REASONABLE_SALARY:
                            valid = False
                    if valid:
                        filtered_results.append(r)
                if filtered_results:
                    best_result = self._select_best_salary_result(filtered_results)
                else:
                    best_result = None

            if best_result:
                currency_from_salary = best_result.get('currency')
                if currency_from_salary and currency_from_salary.strip():
                    df.loc[idx, 'currency_raw'] = currency_from_salary.strip().lower()
                else:
                    df.loc[idx, 'currency_raw'] = None  # will be inferred later if needed

                for k in ['min_salary', 'max_salary', 'single_salary']:
                    v = best_result.get(k)
                    if v is not None and v > MAX_REASONABLE_SALARY:
                        best_result[k] = None
                df.loc[idx, 'has_salary'] = True
                df.loc[idx, 'min_salary_raw'] = best_result['min_salary']
                df.loc[idx, 'max_salary_raw'] = best_result['max_salary']
                df.loc[idx, 'single_salary_raw'] = best_result['single_salary']
                df.loc[idx, 'salary_period'] = best_result['period']

                min_usd, max_usd, avg_usd = self._convert_to_annual_usd(best_result, exchange_rates)
                for col, val in zip(['min_salary_annual_usd', 'max_salary_annual_usd', 'avg_salary_annual_usd'],
                                   [min_usd, max_usd, avg_usd]):
                    if val is not None and val > MAX_REASONABLE_SALARY:
                        df.loc[idx, col] = None
                    else:
                        df.loc[idx, col] = val

                df.loc[idx, 'salary_confidence'] = self._calculate_confidence(best_result)
                # Mark this row as having an extracted salary+currency
                extracted_currency_mask.append(True)
            else:
                df.loc[idx, 'has_salary'] = False
                for col in ['currency_raw', 'min_salary_raw', 'max_salary_raw', 'single_salary_raw',
                            'salary_period', 'min_salary_annual_usd', 'max_salary_annual_usd',
                            'avg_salary_annual_usd', 'salary_confidence']:
                    df.loc[idx, col] = None
                extracted_currency_mask.append(False)

        # --- Standardize all currency values to ISO codes ---
        def standardize_currency(val):
            if not val or str(val).strip() == '' or str(val).lower() == 'none':
                return None
            v = str(val).strip().lower()
            v = v.rstrip('.,')
            # Direct match
            if v in currency_map and currency_map[v]:
                return currency_map[v]
            # Handle common MXN/MX$ patterns
            if v.startswith('mx$') or v.startswith('mxn') or v.startswith('mx '):
                return 'MXN'
            if 'mexican peso' in v or 'mexican pesos' in v:
                return 'MXN'
            # Try to match with spaces removed
            v_nospace = v.replace(' ', '')
            if v_nospace in currency_map and currency_map[v_nospace]:
                return currency_map[v_nospace]
            # ISO code direct
            if v.upper() in iso_codes:
                return v.upper()
            # Try first character (for symbols)
            if v and v[0] in currency_map and currency_map[v[0]]:
                return currency_map[v[0]]
            return None

        df['currency_raw'] = df['currency_raw'].apply(standardize_currency)
        df['currency_raw'] = df['currency_raw'].fillna('USD')

        # --- Fallback: Use geocoding and countryinfo for any remaining missing currency_raw values ---
        try:
            # Only process rows where currency_raw is still missing or empty AND no extracted salary/currency_raw
            missing_currency_mask = (
                (df['currency_raw'].isnull()) | (df['currency_raw'] == '') | (df['currency_raw'].str.lower() == 'none')
            ) & (~pd.Series(extracted_currency_mask))

            locations_to_lookup = df.loc[missing_currency_mask, 'location'].fillna('').unique()

            # Cache for location->country and country->currency
            location_country_cache = {}
            country_currency_cache = {}

            geolocator = Nominatim(user_agent="salary_currency_enricher")

            for loc in locations_to_lookup:
                loc_key = loc.strip().lower()
                if not loc_key:
                    location_country_cache[loc_key] = None
                    continue
                try:
                    geo = geolocator.geocode(loc, language='en', addressdetails=True, timeout=10)
                    time.sleep(1)  # Be nice to the API
                    country = None
                    if geo and hasattr(geo, 'raw'):
                        address = geo.raw.get('address', {})
                        country = address.get('country')
                    location_country_cache[loc_key] = country
                except (KeyError, ValueError, AttributeError, Exception):
                    location_country_cache[loc_key] = None

            for country in set(filter(None, location_country_cache.values())):
                try:
                    info = CountryInfo(country)
                    currencies = info.currencies()
                    if currencies:
                        country_currency_cache[country] = currencies[0]
                    else:
                        country_currency_cache[country] = None
                except (KeyError, ValueError, AttributeError, Exception):
                    country_currency_cache[country] = None

            def geo_currency_fallback(row):
                # Only fallback if currency_raw is missing and no extracted salary/currency_raw
                if row['currency_raw'] and str(row['currency_raw']).strip().upper() not in ('', 'NONE', 'NAN'):
                    return row['currency_raw']
                loc_key = str(row.get('location', '')).strip().lower()
                country = location_country_cache.get(loc_key)
                if country:
                    currency = country_currency_cache.get(country)
                    if currency:
                        return currency.upper()
                return 'USD'
            missing_currency_mask = missing_currency_mask.reindex(df.index, fill_value=False)
            df.loc[missing_currency_mask, 'currency_raw'] = df[missing_currency_mask].apply(geo_currency_fallback, axis=1)
        except ImportError:
            print("Warning: geopy or countryinfo not installed, skipping geocoding fallback for currencies.")

        return df

    def _select_best_salary_result(self, results):
        """Select the most complete salary result from multiple matches"""
        if not results:
            return None

        scored_results = []
        for result in results:
            score = 0
            if result['min_salary'] is not None and result['max_salary'] is not None:
                score += 3
            elif result['single_salary'] is not None:
                score += 2
            if result['currency']:
                score += 2
            if result['period']:
                score += 1
            scored_results.append((score, result))

        return max(scored_results, key=lambda x: x[0])[1]

    def _convert_to_annual_usd(self, salary_result, exchange_rates=None):
        """Convert salary to annual USD equivalent"""
        currency = salary_result['currency']
        period = salary_result['period']
        min_sal = salary_result['min_salary']
        max_sal = salary_result['max_salary']
        single_sal = salary_result['single_salary']

        if exchange_rates is None:
            exchange_rates = self.get_exchange_rates_to_usd()
        exchange_rate = exchange_rates.get(currency, 1.0)
        period_multiplier = 1  # Default to annual

        if period:
            pl = period.lower()
            if any(term in pl for term in ['month', 'monthly', 'per month', 'p.m.', 'pm']):
                period_multiplier = 12
            elif any(term in pl for term in ['hour', 'hourly', 'per hour', 'p.h.', 'ph']):
                period_multiplier = 40 * 52
            elif any(term in pl for term in ['week', 'weekly', 'per week', 'p.w.', 'pw']):
                period_multiplier = 52
            elif any(term in pl for term in ['day', 'daily', 'per day']):
                period_multiplier = 260

        min_annual_usd = None
        max_annual_usd = None
        avg_annual_usd = None

        if min_sal is not None and max_sal is not None:
            min_annual_usd = min_sal * period_multiplier * exchange_rate
            max_annual_usd = max_sal * period_multiplier * exchange_rate
            avg_annual_usd = (min_annual_usd + max_annual_usd) / 2
        elif single_sal is not None:
            avg_annual_usd = single_sal * period_multiplier * exchange_rate
            min_annual_usd = avg_annual_usd
            max_annual_usd = avg_annual_usd

        return min_annual_usd, max_annual_usd, avg_annual_usd

    def _calculate_confidence(self, salary_result):
        """Calculate confidence score for salary extraction (0-1)"""
        score = 0.5
        if salary_result['min_salary'] is not None and salary_result['max_salary'] is not None:
            score += 0.3
        if salary_result['currency']:
            score += 0.1
        if salary_result['period']:
            score += 0.1
        return min(1.0, score)


# Enhanced test cases including million notation and ASEAN-specific formats
def test_salary_extractor():
    """
    Test the SalaryExtractor with a variety of salary formats
    """
    extractor = SalaryExtractor()
    test_texts = [
        # Test for MX$ range
        "The salary range for this role is MX$235,200- MX$252,806.",
        "Software Engineer - Salary: MYR 8,000 - 12,000 per month",
        "We offer $80,000 - $120,000 annually",
        "Compensation: SGD 5,500 monthly",
        "Pay range: RM 4,500 to RM 7,200 per month",
        "Annual salary of USD 95,000",
        "€45,000 - €65,000 per year",
        "Hourly rate: $25 - $35 per hour",
        "Salary up to ¥8,000,000 annually",
        "Competitive salary: INR 12,00,000 - 18,00,000 per annum",
        "Package: 50K - 80K SGD yearly",
        "Starting at THB 45,000 per month",
        "Offering AED 15,000 - AED 25,000 monthly",
        # New test cases for M notation and ASEAN formats
        "Executive package: $1.5M - $2.2M per year",
        "Senior role: USD 500K - 1M annually",
        "PKR 2,50,000 per annum",  # Indian comma style
        "SAR 15,000 per month",
        "Rupiah 3,000,000 per month",
        "Rp 4.500.000 – Rp 7.000.000 monthly",  # European style periods
        "Base salary: €50.000,50 annually",  # European decimal format
        "Compensation 1.2M THB yearly",
        "Starting: IDR 8.5M monthly",
    ]

    print("=== Enhanced Salary Extraction Test Results ===\n")
    for i, text in enumerate(test_texts, 1):
        print(f"Test {i}: {text!r}")
        results = extractor.extract_salaries(text)
        if results:
            for j, result in enumerate(results):
                print(f"  Match {j+1}:")
                print(f"    Full match: {result['full_match']}")
                print(f"    Currency: {result['currency']}")
                if result['min_salary'] is not None and result['max_salary'] is not None:
                    print(f"    Salary range: {result['min_salary']:,.0f} - {result['max_salary']:,.0f}")
                elif result['single_salary'] is not None:
                    print(f"    Single salary: {result['single_salary']:,.0f}")
                print(f"    Period: {result['period']}")
                print(f"    Pattern used: {result['pattern_used']}")
        else:
            print("  No salary found")
        print()

# ETL Pipeline usage example
def demo_etl_pipeline():
    """
    Demonstration of the ETL pipeline using SalaryETL class
    This function creates a sample DataFrame with job postings and applies the SalaryETL process.
    """
    print("\n=== ETL Pipeline Demo ===\n")

    sample_jobs = pd.DataFrame({
        'job_title': [
            'Software Engineer',
            'Data Scientist',
            'Product Manager',
            'Marketing Executive'
        ],
        'job_description': [
            'We are looking for a Software Engineer. Salary: MYR 8,000 - 12,000 monthly. Great benefits included.',
            'Data Science role with competitive package of $120K - $150K annually.',
            'Product Manager position. Compensation up to SGD 10,000 per month.',
            'Marketing role with attractive package. Contact us for details.'
        ],
        'company': ['TechCorp', 'DataLab', 'ProductCo', 'MarketInc']
    })

    etl = SalaryETL()
    processed_df = etl.process_job_dataframe(sample_jobs, text_column='job_description', title_column='job_title')

    salary_cols = ['job_title', 'has_salary', 'currency_raw', 'min_salary_raw',
                   'max_salary_raw', 'salary_period', 'avg_salary_annual_usd', 'salary_confidence']

    print("Processed Results:")
    print(processed_df[salary_cols].to_string(index=False))
    return processed_df

# Run the test and demo ETL pipeline
# Not necessary to run in production, but useful for debugging
# We use SalaryETL as module.
if __name__ == "__main__":
    test_salary_extractor()
    try:
        demo_etl_pipeline()
    except Exception as e:
        print(f"ETL demo failed: {e}")