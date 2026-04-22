"""
Airline Roster PDF Parser — IndiGo / InterGlobe Aviation format
Handles: image-based PDFs via OCR, text-based PDFs via pdfplumber
Duty types: FLIGHT, OFF (OFG/OFB/ROFF/OFF), STANDBY (SBY/SBYP), TRAINING (ERET)
"""
import re
import os
import logging
import tempfile
from datetime import datetime, timedelta
from typing import Optional, List

logger = logging.getLogger(__name__)

# Known duty codes
OFF_CODES = {'OFG', 'OFF', 'OFB', 'ROFF'}
STANDBY_CODES = {'SBY', 'SBYP'}
TRAINING_CODES = {'ERET', 'EREF', 'SIM', 'GROUND'}
ALL_DUTY_CODES = OFF_CODES | STANDBY_CODES | TRAINING_CODES

# Common Indian airports
KNOWN_AIRPORTS = {
    'HYD', 'BLR', 'MAA', 'DEL', 'BOM', 'CCU', 'GOX', 'GOI', 'COK', 'AMD',
    'PNQ', 'JAI', 'GAU', 'PAT', 'BBI', 'IXR', 'NAG', 'IDR', 'VNS', 'LKO',
    'SXR', 'IXC', 'ATQ', 'TRV', 'CJB', 'RPR', 'IXB', 'VTZ', 'DED', 'JSA',
    'BHO', 'RAJ', 'UDR', 'JDH', 'KNU', 'IMF', 'DIB', 'AJL', 'IXA', 'DMU',
    'SHL', 'IXS', 'CIB', 'BDQ', 'STV', 'IXE', 'GOA',
}


def extract_text_from_pdf(file_path: str) -> str:
    """Extract text from PDF, using OCR if text layer is empty."""
    import pdfplumber

    # Try text extraction first
    full_text = ""
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            full_text += page_text + "\n"

    if full_text.strip():
        logger.info(f"Text extraction successful: {len(full_text)} chars")
        return full_text

    # Fall back to OCR
    logger.info("No text layer found, falling back to OCR...")
    return extract_text_via_ocr(file_path)


def extract_text_via_ocr(file_path: str) -> str:
    """Use OCR (tesseract) to extract text from image-based PDF."""
    from pdf2image import convert_from_path
    import pytesseract

    full_text = ""

    # Page 1 grid: use higher DPI + psm 4 for better grid extraction
    images_p1 = convert_from_path(file_path, dpi=300, first_page=1, last_page=1)
    if images_p1:
        text_p1 = pytesseract.image_to_string(images_p1[0], config='--psm 4 --oem 3')
        full_text += text_p1 + "\n"
        logger.info(f"OCR page 1 (300dpi): {len(text_p1)} chars")

    # Remaining pages: 150 DPI is sufficient for tabular crew data
    all_images = convert_from_path(file_path, dpi=150)
    for i, img in enumerate(all_images):
        if i == 0:
            continue  # Already processed page 1 at higher DPI
        text = pytesseract.image_to_string(img)
        full_text += text + "\n"
        logger.info(f"OCR page {i+1} (150dpi): {len(text)} chars")

    return full_text


def extract_date_range(text: str) -> tuple:
    """Extract start/end dates from header."""
    match = re.search(r'(\d{2}/\d{2}/\d{4})\s*[-–]\s*(\d{2}/\d{2}/\d{4})', text)
    if match:
        start = datetime.strptime(match.group(1), '%d/%m/%Y')
        end = datetime.strptime(match.group(2), '%d/%m/%Y')
        return start, end
    return None, None


def extract_metadata(text: str) -> dict:
    """Extract crew member info."""
    metadata = {}

    # Employee: 46107 T, RAMPRASAD or 46107 - T, RAMPRASAD
    emp_match = re.search(r'(\d{4,6})\s*[-–]?\s*([A-Z]),\s*([A-Z]+)', text)
    if emp_match:
        metadata['employee_id'] = emp_match.group(1)
        metadata['crew_name'] = f"{emp_match.group(3)}, {emp_match.group(2)}"

    # Base-Position-Aircraft: HYD-FO-320
    base_match = re.search(r'([A-Z]{3})\s*[-–]\s*([A-Z]{2,3})\s*[-–]\s*(\d{3})', text)
    if base_match:
        metadata['base'] = base_match.group(1)
        metadata['position'] = base_match.group(2)
        metadata['aircraft_qual'] = base_match.group(3)

    # Period
    range_match = re.search(r'(\d{2}/\d{2}/\d{4})\s*[-–]\s*(\d{2}/\d{2}/\d{4})', text)
    if range_match:
        metadata['period'] = f"{range_match.group(1)} - {range_match.group(2)}"

    return metadata


def parse_indigo_roster(full_text: str) -> list:
    """
    Parse IndiGo roster. Strategy:
    1. Extract flight-date assignments from crew detail pages (reliable)
    2. For non-flight days, detect duty codes from page 1 grid
    3. Blank days = OFF
    """
    start_date, end_date = extract_date_range(full_text)
    if not start_date or not end_date:
        logger.error("Cannot determine date range")
        return []

    lines = full_text.split('\n')
    upper_text = full_text.upper()

    # ── Step 1: Extract flight-date assignments from crew details (Pages 2+) ──
    # Pattern: "DD/MM/YYYY FLIGHT_NUM" (with possible spaces/OCR artifacts)
    date_flights = {}
    
    # Also track training detail lines to filter false positives
    training_times = set()
    for line in lines:
        line_stripped = line.strip()
        # Detect training lines: "14/04/2026 1352-1501" or "14/04/2026 1616-1730"
        training_match = re.search(r'(\d{2}/\d{2}/\d{4})\s+(\d{3,4})\s*[-–]\s*(\d{3,4})', line_stripped)
        if training_match and ('Observer' in line or 'FAM' in line or 'Training' in line.lower() or 'ERET' in line.upper()):
            training_times.add(training_match.group(2))
            training_times.add(training_match.group(3))
            continue

    for line in lines:
        line_stripped = line.strip()
        # Match: 01/04/2026 6405 or 18/04/2026 6242 or variations
        match = re.match(r'(\d{2}/\d{2}/\d{4})\s+(\d{2,5})\b', line_stripped)
        if match:
            try:
                date_obj = datetime.strptime(match.group(1), '%d/%m/%Y')
                date_str = date_obj.strftime('%Y-%m-%d')
                flight_num = match.group(2)
                # Filter: must be valid flight number range and not a training time
                if 100 <= int(flight_num) <= 99999 and flight_num not in training_times:
                    # Skip if line contains training keywords
                    if any(kw in line_stripped.upper() for kw in ['OBSERVER', 'FAM', 'TRAINEE', 'HOTEL', 'MEMO']):
                        continue
                    if date_str not in date_flights:
                        date_flights[date_str] = []
                    if flight_num not in date_flights[date_str]:
                        date_flights[date_str].append(flight_num)
            except ValueError:
                continue

    logger.info(f"Found flight assignments for {len(date_flights)} dates: {date_flights}")

    # ── Step 2: Detect airport routes from page 1 grid ──
    # The grid text has airport codes in sequence. We'll try to extract them
    flight_airports = {}  # flight_num -> (dep, arr)

    # Scan for lines with airport codes (3 uppercase letters in KNOWN_AIRPORTS)
    airport_sequences = []
    for line in lines:
        airports_in_line = re.findall(r'\b([A-Z]{3})\b', line)
        known = [a for a in airports_in_line if a in KNOWN_AIRPORTS]
        if len(known) >= 2:
            airport_sequences.extend(known)

    # ── Step 3: Detect duty codes per date ──
    # Strategy: Find the duty code row in page 1 (OCR often garbles codes)
    # Then map codes to dates by position order
    date_duty_codes = {}

    # Extract page 1 text
    page1_end = upper_text.find('OTHER CREW')
    if page1_end == -1:
        page1_end = upper_text.find('DETAILS')
    if page1_end == -1:
        page1_end = len(upper_text) // 3
    page1_text = upper_text[:page1_end]

    # OCR fuzzy matching for duty codes
    # OFG often OCR'd as: OFG, OFC, OFA, ofc
    # SBY often OCR'd as: SBY, SAY, SPY, SOY, SEY, SEV
    # SBYP often OCR'd as: SBYP, SEVP, SAVE, SEVE, SBVP
    # ERET often OCR'd as: ERET, ENER, ERET
    # ROFF often OCR'd as: ROFF, POFF, BORE, BOFF
    # OFB often OCR'd as: OFB, OFA, OFC

    fuzzy_off = re.compile(r'\b(OFG|OFC|OFA|OFF|ROFF|POFF|ROF|OFB)\b', re.IGNORECASE)
    fuzzy_standby = re.compile(r'\b(SBY|SBYP|SAY|SPY|SOY|SEY|SEV|SEVE|SEVP|SAVE|SBVP)\b', re.IGNORECASE)
    fuzzy_training = re.compile(r'\b(ERET|ENER|EREF)\b', re.IGNORECASE)

    # Find the duty code row in page 1
    # It's the row that contains the most duty code-like tokens
    best_line = ""
    best_score = 0
    for line in page1_text.split('\n'):
        score = len(fuzzy_off.findall(line)) + len(fuzzy_standby.findall(line)) + len(fuzzy_training.findall(line))
        if score > best_score:
            best_score = score
            best_line = line

    if best_line and best_score >= 3:
        logger.info(f"Found duty code row with {best_score} codes: {best_line[:200]}")

        # Parse the code row: split by delimiters (|, spaces around codes)
        # Extract tokens in order — they correspond to dates in order
        tokens = re.split(r'[|]', best_line)
        # Flatten and clean
        all_tokens = []
        for t in tokens:
            parts = t.strip().split()
            all_tokens.extend(parts)

        # Map tokens to dates
        date_idx = 0
        current = start_date
        num_days = (end_date - start_date).days + 1

        for token in all_tokens:
            if date_idx >= num_days:
                break
            token_upper = token.upper().strip('_').strip()
            if not token_upper or len(token_upper) < 2:
                continue

            date_str = (start_date + timedelta(days=date_idx)).strftime('%Y-%m-%d')

            # Check if this token is a time (HH:MM) → skip, it's a reporting time for a flight day
            if re.match(r'^\d{1,2}[:.]\d{2}$', token_upper):
                date_idx += 1
                continue

            # Check for fuzzy duty codes
            if fuzzy_off.match(token_upper):
                code = token_upper
                # Determine specific code
                if code in ('OFG', 'OFC', 'OFA'):
                    date_duty_codes[date_str] = ('OFF', 'OFG')
                elif code in ('ROFF', 'POFF', 'ROF'):
                    date_duty_codes[date_str] = ('OFF', 'ROFF')
                elif code in ('OFB',):
                    date_duty_codes[date_str] = ('OFF', 'OFB')
                else:
                    date_duty_codes[date_str] = ('OFF', code)
                date_idx += 1
            elif fuzzy_standby.match(token_upper):
                sbyp = len(token_upper) > 3  # SBYP variants have 4+ chars
                date_duty_codes[date_str] = ('STANDBY', 'SBYP' if sbyp else 'SBY', [])
                date_idx += 1
            elif fuzzy_training.match(token_upper):
                date_duty_codes[date_str] = ('TRAINING', 'ERET')
                date_idx += 1
            elif re.match(r'^\d{3,5}$', token_upper):
                # Flight number — skip, handled by crew details
                date_idx += 1
            else:
                # Unknown token — could be garbled, skip
                pass

    logger.info(f"Detected duty codes for {len(date_duty_codes)} dates: { {k: v[:2] for k, v in date_duty_codes.items()} }")

    # ── Step 4: Check training details section ──
    training_dates = set()
    training_match = re.findall(r'(\d{2}/\d{2}/\d{4})\s+.*(?:Observer|FAM|Training|ERET)', full_text, re.IGNORECASE)
    for tm in training_match:
        try:
            dt = datetime.strptime(tm, '%d/%m/%Y').strftime('%Y-%m-%d')
            training_dates.add(dt)
        except ValueError:
            pass

    # ── Step 5: Build final duty list ──
    duties = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')

        if date_str in date_flights:
            # Flight day
            flights = date_flights[date_str]
            sectors = []
            for fn in flights:
                sector = {
                    'flight_number': fn,
                    'duty_type': 'FLIGHT',
                    'departure_airport_iata': None,
                    'arrival_airport_iata': None,
                    'aircraft_type': None,
                    'overall_confidence': 0.85,
                }
                sectors.append(sector)

            # Check if also a training day (Observer/FAM)
            is_training_flight = date_str in training_dates
            duties.append({
                'date': date_str,
                'duty_type': 'FLIGHT',
                'duty_code': 'OBS' if is_training_flight else None,
                'reporting_time': None,
                'duties': sectors,
                'debrief_time': None,
                'overall_confidence': 0.85,
            })

        elif date_str in date_duty_codes:
            # Known duty code day
            code_info = date_duty_codes[date_str]
            duty_type = code_info[0]
            duty_code = code_info[1]
            reporting_time = None
            debrief_time = None

            if duty_type == 'STANDBY' and len(code_info) > 2 and code_info[2]:
                times = code_info[2]
                reporting_time = times[0] if times else None
                debrief_time = times[-1] if len(times) > 1 else None

            duties.append({
                'date': date_str,
                'duty_type': duty_type,
                'duty_code': duty_code,
                'reporting_time': reporting_time,
                'duties': [],
                'debrief_time': debrief_time,
                'overall_confidence': 0.8,
            })

        else:
            # No flight, no code found — mark as OFF (blank day)
            duties.append({
                'date': date_str,
                'duty_type': 'OFF',
                'duty_code': 'BLANK',
                'reporting_time': None,
                'duties': [],
                'debrief_time': None,
                'overall_confidence': 0.6,
            })

        current += timedelta(days=1)

    return duties


def parse_roster_pdf_text(full_text: str) -> dict:
    """Entry point: detect format and parse."""
    upper = full_text.upper()

    is_indigo = 'INTERGLOBE' in upper or 'INDIGO' in upper or '6E' in upper
    is_crew_schedule = 'CREW SCHEDULE' in upper or 'PERSONAL CREW' in upper

    if is_indigo or is_crew_schedule:
        duties = parse_indigo_roster(full_text)
        format_name = 'indigo'
    else:
        duties = parse_generic_roster(full_text)
        format_name = 'generic'

    metadata = extract_metadata(full_text)

    flight_days = sum(1 for d in duties if d['duty_type'] == 'FLIGHT')
    off_days = sum(1 for d in duties if d['duty_type'] == 'OFF')
    standby_days = sum(1 for d in duties if d['duty_type'] == 'STANDBY')
    training_days = sum(1 for d in duties if d['duty_type'] == 'TRAINING')

    block_hours = None
    bh_match = re.search(r'Block\s*Hours\D*(\d+:\d+)', full_text, re.IGNORECASE)
    if bh_match:
        block_hours = bh_match.group(1)

    return {
        'duties': duties,
        'metadata': metadata,
        'stats': {
            'total_days': len(duties),
            'flight_days': flight_days,
            'off_days': off_days,
            'standby_days': standby_days,
            'training_days': training_days,
            'block_hours': block_hours,
        },
        'debug': {
            'text_length': len(full_text),
            'text_snippet': full_text[:500],
            'format_detected': format_name,
        }
    }


def parse_generic_roster(full_text: str) -> list:
    """Fallback parser for non-IndiGo formats."""
    duties = []
    lines = full_text.split('\n')
    current_date = None
    current_flights = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        date_match = re.match(r'^(\d{1,2}[/\-.]\d{1,2}[/\-.]\d{2,4})', line)
        if date_match:
            if current_date and current_flights:
                duties.append({
                    'date': current_date,
                    'duty_type': 'FLIGHT',
                    'duties': current_flights,
                    'overall_confidence': 0.5,
                })
            current_date = date_match.group(1)
            current_flights = []

            upper_line = line.upper()
            for code in OFF_CODES:
                if code in upper_line:
                    duties.append({'date': current_date, 'duty_type': 'OFF', 'duty_code': code, 'duties': []})
                    current_date = None
                    break
            for code in STANDBY_CODES:
                if code in upper_line:
                    duties.append({'date': current_date, 'duty_type': 'STANDBY', 'duty_code': code, 'duties': []})
                    current_date = None
                    break

        if current_date:
            flight_nums = re.findall(r'\b(\d{2,5})\b', line)
            valid = [f for f in flight_nums if 100 <= int(f) <= 99999]
            for fn in valid:
                current_flights.append({
                    'flight_number': fn,
                    'duty_type': 'FLIGHT',
                    'overall_confidence': 0.4,
                })

    if current_date and current_flights:
        duties.append({
            'date': current_date,
            'duty_type': 'FLIGHT',
            'duties': current_flights,
            'overall_confidence': 0.5,
        })

    return duties
