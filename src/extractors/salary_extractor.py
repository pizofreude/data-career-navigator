# -*- coding: utf-8 -*-
# src\salary_extractor.py

"""
File: salary_extractor.py
--------------------------
Enhanced Salary Extractor and ETL Pipeline.
This module provides a comprehensive salary extraction tool that can handle various currency formats,
salary ranges, and different salary periods. It includes an ETL pipeline to process job postings and
automatically extract salary information into a structured format.

Usage: You can run this module directly.
Note: The test_salary_extractor() function verifies a wide range of salary formats,
and demo_etl_pipeline() shows how to apply SalaryETL to a sample DataFrame for production use.
"""

import re
from typing import List
import pandas as pd
import requests
import time
import os
import json

class SalaryExtractor:
    """
    Extractor for salary information from job postings.
    This class uses regular expressions to identify and extract salary details.
    """
    def __init__(self):
        # Major currency symbols and codes
        self.currency_symbols = {
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
        # Supports: 50000, 50,000, 50.000, 50K, 50k, 5.5K, 1.5M, etc.
        number_pattern = r'(?:\d{1,3}(?:[.,]\s?\d{3})*(?:\.\d{1,2})?[kKmM]?|\d+(?:\.\d{1,2})?[kKmM]?)'

        # Currency symbol/code pattern (optional whitespace)
        currency_prefix = rf'(?:({currency_pattern})\s*)'
        currency_suffix = rf'(?:\s*({currency_pattern}))'

        # Build comprehensive patterns
        self.patterns = [
            # Pattern 1: Currency prefix with range (e.g., $50,000 - $80,000)
            rf'{currency_prefix}?({number_pattern})\s*[-–—to]\s*{currency_prefix}?({number_pattern})(?:\s*({period_pattern}))?',

            # Pattern 2: Currency suffix with range (e.g., 50,000 - 80,000 USD)
            rf'({number_pattern})\s*[-–—to]\s*({number_pattern})\s*{currency_suffix}(?:\s*({period_pattern}))?',

            # Pattern 3: Single salary with currency prefix (e.g., $75,000 annually)
            rf'{currency_prefix}({number_pattern})(?:\s*({period_pattern}))?',

            # Pattern 4: Single salary with currency suffix (e.g., 75,000 USD per year)
            rf'({number_pattern})\s*{currency_suffix}(?:\s*({period_pattern}))?',

            # Pattern 5: Salary range without explicit currency in between
            rf'({number_pattern})\s*[-–—to]\s*({number_pattern})\s*{currency_suffix}(?:\s*({period_pattern}))?',

            # Pattern 6: Complex patterns like "Salary: MYR 5,000 - 8,000"
            rf'(?:salary|compensation|pay|wage|income)[:]\s*{currency_prefix}?({number_pattern})\s*[-–—to]\s*{currency_prefix}?({number_pattern})(?:\s*({period_pattern}))?',
        ]

        # Compile all patterns (case-insensitive)
        self.compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.patterns]

    def extract_salaries(self, text: str) -> List[dict]:
        """
        Extract salary information from text
        Returns list of dictionaries with salary details
        """
        results = []
        for i, pattern in enumerate(self.compiled_patterns):
            matches = pattern.finditer(text)
            for match in matches:
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

                groups = match.groups()

                # Extract currency (look for non-None currency groups)
                for group in groups:
                    if group and any(curr.lower() in group.lower() for curr in self.currency_symbols):
                        salary_info['currency'] = group.upper().strip()
                        break

                # Extract salary amounts and period
                salary_numbers = []
                for group in groups:
                    if group and self._is_number(group):
                        salary_numbers.append(group)
                    elif group and any(period.lower() in group.lower() for period in self.period_indicators):
                        salary_info['period'] = group.lower().strip()

                # Assign salary values
                if len(salary_numbers) >= 2:
                    salary_info['min_salary'] = self._normalize_number(salary_numbers[0])
                    salary_info['max_salary'] = self._normalize_number(salary_numbers[1])
                elif len(salary_numbers) == 1:
                    salary_info['single_salary'] = self._normalize_number(salary_numbers[0])

                if salary_info['currency'] or salary_info['min_salary'] or salary_info['single_salary']:
                    results.append(salary_info)

        # Remove duplicates and overlapping matches
        return self._deduplicate_results(results)

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
            with open(json_path, "r") as f:
                rates = json.load(f)
            # Only keep rates for known currencies (for backward compatibility)
            known_currencies = [
                'USD', 'MYR', 'SGD', 'EUR', 'GBP', 'INR', 'THB', 'IDR', 'PHP',
                'VND', 'CNY', 'JPY', 'AUD', 'CAD', 'CHF', 'HKD', 'KRW',
                'AED', 'SAR', 'QAR', 'KWD',
            ]
            filtered_rates = {k: rates[k] for k in known_currencies if k in rates}
            filtered_rates['USD'] = 1.0
            self._exchange_rates_cache = filtered_rates
            self._exchange_rates_cache_time = now
            return filtered_rates
        except Exception as e:
            print(f"Warning: Could not load exchange rates from exchange_rates_usd.json: {e}")

        # Fallback to static rates if file is missing or invalid
        return {
            'USD': 1.0, 'MYR': 0.21, 'SGD': 0.74, 'EUR': 1.09, 'GBP': 1.27,
            'INR': 0.012, 'THB': 0.028, 'IDR': 0.000066, 'PHP': 0.018,
            'VND': 0.000040, 'CNY': 0.14, 'JPY': 0.0067, 'AUD': 0.66,
            'CAD': 0.73, 'CHF': 1.11, 'HKD': 0.13, 'KRW': 0.00075,
            'AED': 0.27, 'SAR': 0.27, 'QAR': 0.27, 'KWD': 3.28,
        }

    def process_job_dataframe(self, df, text_column='job_description', include_title=True, title_column='job_title'):
        """
        Process a DataFrame of job posts to extract salary information

        Args:
            df: DataFrame with job posts
            text_column: Column containing job description text
            include_title: Whether to include job title in salary search
            title_column: Column containing job titles

        Returns:
            DataFrame with added salary columns
        """
        df = df.copy()
        salary_columns = [
            'has_salary', 'currency', 'min_salary_raw', 'max_salary_raw',
            'single_salary_raw', 'salary_period', 'min_salary_annual_usd',
            'max_salary_annual_usd', 'avg_salary_annual_usd', 'salary_confidence'
        ]
        for col in salary_columns:
            df[col] = None

        exchange_rates = self.get_exchange_rates_to_usd()
        for idx, row in df.iterrows():
            text_to_search = ''
            if pd.notna(row.get(text_column, '')):
                text_to_search = row[text_column]
            if include_title and title_column in df.columns and pd.notna(row.get(title_column, '')):
                text_to_search = f"{row[title_column]} {text_to_search}"

            salary_results = self.extractor.extract_salaries(text_to_search)
            if salary_results:
                best_result = self._select_best_salary_result(salary_results)
                df.loc[idx, 'has_salary'] = True
                df.loc[idx, 'currency'] = best_result['currency']
                df.loc[idx, 'min_salary_raw'] = best_result['min_salary']
                df.loc[idx, 'max_salary_raw'] = best_result['max_salary']
                df.loc[idx, 'single_salary_raw'] = best_result['single_salary']
                df.loc[idx, 'salary_period'] = best_result['period']

                min_usd, max_usd, avg_usd = self._convert_to_annual_usd(best_result, exchange_rates)
                df.loc[idx, 'min_salary_annual_usd'] = min_usd
                df.loc[idx, 'max_salary_annual_usd'] = max_usd
                df.loc[idx, 'avg_salary_annual_usd'] = avg_usd

                df.loc[idx, 'salary_confidence'] = self._calculate_confidence(best_result)
            else:
                df.loc[idx, 'has_salary'] = False
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
        print(f"Test {i}: {text}")
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

    salary_cols = ['job_title', 'has_salary', 'currency', 'min_salary_raw',
                   'max_salary_raw', 'salary_period', 'avg_salary_annual_usd', 'salary_confidence']

    print("Processed Results:")
    print(processed_df[salary_cols].to_string(index=False))
    return processed_df

if __name__ == "__main__":
    test_salary_extractor()
    try:
        demo_etl_pipeline()
    except Exception as e:
        print(f"ETL demo failed: {e}")