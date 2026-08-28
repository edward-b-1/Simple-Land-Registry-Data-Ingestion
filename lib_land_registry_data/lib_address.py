
# SQL for normalising the Price Paid address fields PAON (primary addressable
# object name) and SAON (secondary addressable object name) into separate
# columns. The raw fields mix several conventions, e.g.
#
#   PAON '42'                  SAON ''          -> building_number 42
#   PAON '12A'                 SAON 'FLAT 3'    -> building_number 12A, flat_number 3
#   PAON 'MILNER COURT, 9'     SAON 'FLAT 1'    -> building_name MILNER COURT, building_number 9, flat_number 1
#   PAON 'MILNER COURT'        SAON '2' (house) -> building_name MILNER COURT, building_number 2
#   PAON 'MILNER COURT'        SAON '2' (flat)  -> building_name MILNER COURT, flat_number 2
#   PAON 'FAIRHAVEN'           SAON ''          -> building_name FAIRHAVEN
#   PAON '4 MANOR FARM BARNS'  SAON ''          -> building_number 4, building_name MANOR FARM BARNS
#   PAON '3 ALDHURST ROW, 19'  SAON ''          -> building_number 19, building_name ALDHURST ROW, unit 3 (flat 3 if a flat)
#   PAON 'FLAT 1, 33'          SAON ''          -> building_number 33, flat_number 1
#   PAON 'UNIT 16'             SAON ''          -> unit_description UNIT 16
#   PAON 'PLOT 4'              SAON ''          -> plot_number 4
#   PAON ''                    SAON '18'        -> building_number 18
#   PAON '171'                 SAON 'GROUND FLOOR FLAT AT' -> building_number 171, flat_description GROUND FLOOR FLAT
#
# `is_flat_like` is true when property_type is F or the address itself says
# flat/apartment (the source sometimes types a flat as T/S/D). Flat rules key
# off it rather than off property_type. `flat_number` / `flat_description`
# are filled for flat-like rows, `unit_description` (UNIT 4, GARAGE 2, ...)
# for the rest. `address_pattern` records which PAON and SAON rule fired
# (e.g. 'P_NAME_NUMBER/S_FLAT') so the rules can be audited and refined.
#
# The SQL is in three stages, each a select-list fragment that is spliced
# into a CTE by main_pp_transactions.py:
#   ADDRESS_PARTS_SQL          -- aliases paon, saon, property_type in scope
#   ADDRESS_NORMALISATION_SQL  -- the stage 1 columns in scope
#   PROPERTY_KEY_NORMALISED_SQL -- the stage 2 columns and postcode in scope
# The fragments contain no '%' characters, so they are safe inside a psycopg
# query that uses %(name)s parameters.

# --- building blocks ----------------------------------------------------

_NUMBER = r"[0-9]+[A-Z]?"
_RANGE_SEP = r" *(?:-|/|TO|&|AND) *"
_RANGE = rf"{_NUMBER}{_RANGE_SEP}{_NUMBER}"
_NUMBER_OR_RANGE = rf"{_NUMBER}(?:{_RANGE_SEP}{_NUMBER})?"

# a flat identifier: contains a digit ('3', '14A', 'G.03', 'A12') or is a single letter ('D')
_FLAT_ID = r"(?:[A-Z]?[0-9]+[0-9A-Z.]*|[A-Z][0-9.]*[0-9][0-9A-Z.]*|[A-Z])"

_FLAT_WORD = r"(?:FLAT|FLATS|APARTMENT|APARTMENTS|APARTMANT|APPARTMENT|APT|UNIT|PENTHOUSE|MAISONETTE|STUDIO)"

# words that describe a sub-unit rather than name a building
_GENERIC_UNIT_WORD = (
    r"(?:UNIT|UNITS|GARAGE|GARAGES|BLOCK|PARKING SPACE|PARKING SPACES|PARKING|CAR PARK|CAR PARKING SPACE"
    r"|STORE|STOREROOM|STORAGE UNIT|STORAGE|WORKSHOP|BAY|BERTH|PITCH|CHALET|HUT|BEACH HUT|STALL|KIOSK|LOCK UP"
    r"|PARCEL|SITE|LAND|FLAT|FLATS|APARTMENT|APARTMENTS|MAISONETTE|ROOM|ROOMS|OFFICE|OFFICES|SHOP|SHOPS|PART)"
)
# the word on its own ('GARAGE', 'MAISONETTE') or followed by an identifier
# containing a digit ('UNIT 16', 'BLOCK 1 THE HICKING BUILDING'); 'BAY
# APARTMENTS' or 'LAND ASSOCIATED WITH 60' are names, not units
_GENERIC_UNIT = rf"^{_GENERIC_UNIT_WORD}(?: +[A-Z]?[0-9][0-9A-Z.-]*(?: .*)?)?$"


def _normalise_range(expression: str) -> str:
    # '54 - 56' / '54 TO 56' / '54/56' -> '54-56'
    return f"regexp_replace(regexp_replace({expression}, ' *(-|/|TO|&|AND) *', '-'), ' ', '', 'g')"


def _normalise_number(expression: str) -> str:
    # strip leading zeros ('001' -> '1', '0077-0069' -> '77-69') and put a
    # reversed all-digit range the right way round ('88-86' -> '86-88')
    stripped = rf"regexp_replace({expression}, '(^|-)0+([0-9])', '\1\2', 'g')"
    return f"""
    case
        when ({stripped}) ~ '^[0-9]+-[0-9]+$'
         and split_part({stripped}, '-', 1)::bigint > split_part({stripped}, '-', 2)::bigint
            then split_part({stripped}, '-', 2) || '-' || split_part({stripped}, '-', 1)
        else {stripped}
    end
    """


def _normalise_flat(expression: str) -> str:
    # strip leading zeros from all-digit flat numbers ('001' -> '1'), leave
    # floor-coded or lettered ones alone ('0501' stays, 'A001' stays)
    return rf"""
    case
        when ({expression}) ~ '^0+[0-9]+$' and length({expression}) <= 3 then regexp_replace({expression}, '^0+([0-9])', '\1')
        else {expression}
    end
    """


# --- PAON patterns -------------------------------------------------------

PAON_NUMBER = rf"^{_NUMBER}$"
PAON_RANGE = rf"^{_RANGE}$"
# (no UNIT here: 'UNIT 16' as a PAON is a generic sub-unit, not a flat)
_PAON_FLAT_WORD = r"(?:FLAT|FLATS|APARTMENT|APARTMENTS|APARTMANT|APPARTMENT|APT|PENTHOUSE|MAISONETTE|STUDIO)"
PAON_FLAT = rf"^{_PAON_FLAT_WORD} +{_FLAT_ID}(?:, *{_NUMBER_OR_RANGE})?$"  # 'FLAT 1, 33', 'APARTMENT 14'
PAON_PLOT = r"^PLOTS? +.+$"
PAON_UNIT_NAME_NUMBER = rf"^{_NUMBER} +[A-Z].*, *{_NUMBER_OR_RANGE}$"      # '3 ALDHURST ROW, 19'
PAON_NAME_NUMBER = rf"^.+, *{_NUMBER_OR_RANGE}$"                            # 'MILNER COURT, 9'
PAON_GENERIC = _GENERIC_UNIT                                                # 'UNIT 16', 'GARAGE 14', 'BLOCK 1 ...'
PAON_NUMBER_NAME = rf"^{_NUMBER_OR_RANGE} +[A-Z].*$"                        # '4 MANOR FARM BARNS'
PAON_NAME_RANGE = rf"^.+[A-Z] +{_RANGE}$"                                   # 'RIVERSIDE LOFTS 35-36'

PAON_PATTERN_SQL = f"""
    case
        when paon = '' then 'P_EMPTY'
        when paon ~ '{PAON_NUMBER}' then 'P_NUMBER'
        when paon ~ '{PAON_RANGE}' then 'P_RANGE'
        when paon ~ '{PAON_FLAT}' then 'P_FLAT'
        when paon ~ '{PAON_PLOT}' then 'P_PLOT'
        when paon ~ '{PAON_UNIT_NAME_NUMBER}' then 'P_UNIT_NAME_NUMBER'
        when paon ~ '{PAON_NAME_NUMBER}' then 'P_NAME_NUMBER'
        when paon ~ '{PAON_GENERIC}' then 'P_GENERIC'
        when paon ~ '{PAON_NUMBER_NAME}' then 'P_NUMBER_NAME'
        when paon ~ '{PAON_NAME_RANGE}' then 'P_NAME_RANGE'
        when paon ~ '[0-9]' then 'P_OTHER'
        else 'P_NAME'
    end
"""

_PAON_TRAILING_NUMBER = rf", *({_NUMBER_OR_RANGE})$"      # the ', 9' of 'MILNER COURT, 9'
_PAON_LEADING_NUMBER = rf"^({_NUMBER_OR_RANGE}) +[A-Z]"   # the '4 ' of '4 MANOR FARM BARNS'
_PAON_TRAILING_RANGE = rf"[A-Z] +({_RANGE})$"             # the '35-36' of 'RIVERSIDE LOFTS 35-36'

# the street number part of the PAON, before normalisation
PAON_NUMBER_RAW_SQL = f"""
    case
        when paon ~ '{PAON_NUMBER}' then paon
        when paon ~ '{PAON_RANGE}' then {_normalise_range('paon')}
        when paon ~ '{PAON_FLAT}' and paon ~ ',' then {_normalise_range(f"substring(paon from '{_PAON_TRAILING_NUMBER}')")}
        when paon ~ '{PAON_PLOT}' then null
        when paon ~ '{PAON_UNIT_NAME_NUMBER}' or paon ~ '{PAON_NAME_NUMBER}' then {_normalise_range(f"substring(paon from '{_PAON_TRAILING_NUMBER}')")}
        when paon ~ '{PAON_GENERIC}' then null
        when paon ~ '{PAON_NUMBER_NAME}' then {_normalise_range(f"substring(paon from '{_PAON_LEADING_NUMBER}')")}
        when paon ~ '{PAON_NAME_RANGE}' then {_normalise_range(f"substring(paon from '{_PAON_TRAILING_RANGE}')")}
    end
"""

# the name part of the PAON (may still be a generic unit word, filtered in stage 2)
PAON_NAME_RAW_SQL = f"""
    case
        when paon = '' then null
        when paon ~ '{PAON_NUMBER}' or paon ~ '{PAON_RANGE}' or paon ~ '{PAON_FLAT}' then null
        when paon ~ '{PAON_PLOT}' then nullif(btrim(substring(paon from '^PLOTS? +[0-9A-Z]+(?:{_RANGE_SEP}[0-9A-Z]+)? +(.+)$')), '')
        when paon ~ '{PAON_UNIT_NAME_NUMBER}' then btrim(regexp_replace(regexp_replace(paon, '{_PAON_TRAILING_NUMBER}', ''), '^{_NUMBER} +', ''))
        when paon ~ '{PAON_NAME_NUMBER}' then btrim(regexp_replace(paon, '{_PAON_TRAILING_NUMBER}', ''))
        when paon ~ '{PAON_GENERIC}' then null
        when paon ~ '{PAON_NUMBER_NAME}' then btrim(regexp_replace(paon, '^{_NUMBER_OR_RANGE} +', ''))
        when paon ~ '{PAON_NAME_RANGE}' then btrim(regexp_replace(paon, ' +{_RANGE}$', ''))
        else paon
    end
"""

# the leading unit number of '3 ALDHURST ROW, 19'
PAON_LEADING_UNIT_SQL = f"""
    case
        when paon ~ '{PAON_UNIT_NAME_NUMBER}' then substring(paon from '^({_NUMBER}) ')
    end
"""

# a flat number given in the PAON ('FLAT 1, 33', 'FLAT D4', or 'UNIT 3' on a flat)
PAON_FLAT_RAW_SQL = f"""
    case
        when paon ~ '{PAON_FLAT}' then substring(paon from '^{_PAON_FLAT_WORD} +({_FLAT_ID})')
        when paon ~ '{PAON_GENERIC}' then substring(paon from '^{_GENERIC_UNIT_WORD} +({_FLAT_ID})(?: |$)')
    end
"""

# 'PLOT 4' -> 4, 'PLOTS 1-8' -> 1-8, 'PLOTS H1 TO H3' -> H1-H3
PAON_PLOT_RAW_SQL = f"""
    case
        when paon ~ '{PAON_PLOT}' then {_normalise_range(f"substring(paon from '^PLOTS? +([0-9A-Z]+(?:{_RANGE_SEP}[0-9A-Z]+)?)')")}
    end
"""

# the PAON text that describes a sub-unit rather than a building ('UNIT 16', 'GARAGE 12')
PAON_UNIT_TEXT_SQL = f"""
    case
        when paon ~ '{PAON_GENERIC}' then paon
    end
"""

# --- SAON patterns -------------------------------------------------------

SAON_FLAT = rf"^{_FLAT_WORD} +{_FLAT_ID}$"                          # 'FLAT 3', 'APARTMENT 14', 'FLAT D', 'UNIT G.03'
SAON_FLAT_LOOSE = rf"{_FLAT_WORD}\.? *([0-9]+[A-Z]?(?:\.[0-9]+)?)"  # 'SECOND FLOOR FLAT 33', 'FLAT 2 ARLINGTON COURT'
SAON_NUMBER = rf"^{_NUMBER}$"
SAON_LETTER = r"^[A-Z]$"
SAON_UNIT = r"^UNITS? +.+$"
SAON_FLOOR = r"(FLOOR|BASEMENT|GROUND|MAISONETTE|ANNEX|PENTHOUSE|GARDEN|TOP|UPPER|LOWER|REAR|FRONT|SIDE)"

# the SAON says this is a flat, whatever property_type says
SAON_FLAT_LIKE = r"(^|[^A-Z])(FLAT|FLATS|APARTMENT|APARTMENTS|APARTMANT|APPARTMENT|APT|MAISONETTE|PENTHOUSE|STUDIO)([^A-Z]|$)"

SAON_PATTERN_SQL = f"""
    case
        when saon = '' then 'S_EMPTY'
        when saon ~ '{SAON_FLAT}' then 'S_FLAT'
        when saon ~ '{SAON_NUMBER}' then 'S_NUMBER'
        when saon ~ '{SAON_LETTER}' then 'S_LETTER'
        when saon ~ '{SAON_UNIT}' then 'S_UNIT'
        when saon ~ '{SAON_FLOOR}' then 'S_FLOOR'
        else 'S_OTHER'
    end
"""

IS_FLAT_LIKE_SQL = f"""
    (property_type = 'F' or saon ~ '{SAON_FLAT_LIKE}' or paon ~ '^(FLAT|FLATS|APARTMENT|APARTMENTS|APARTMANT|APPARTMENT|APT|MAISONETTE|PENTHOUSE|STUDIO)( |$)')
"""

# the flat identifier in the SAON, regardless of property type
SAON_FLAT_NUMBER_SQL = f"""
    case
        when saon ~ '{SAON_FLAT}' then substring(saon from '^{_FLAT_WORD} +({_FLAT_ID})$')
        when saon ~ '{SAON_NUMBER}' or saon ~ '{SAON_LETTER}' then saon
        when saon ~ '{SAON_FLAT_LOOSE}' then substring(saon from '{SAON_FLAT_LOOSE}')
    end
"""

# a descriptive SAON ('FIRST FLOOR FLAT', 'GARDEN FLAT AT' -> 'GARDEN FLAT'); a
# bare 'FLAT' carries no information and becomes NULL
SAON_DESCRIPTION_SQL = f"""
    nullif(
        case
            when saon = '' or saon ~ '{SAON_FLAT}' or saon ~ '{SAON_NUMBER}' or saon ~ '{SAON_LETTER}' then null
            else regexp_replace(regexp_replace(saon, ' +AT$', ''), '^(THE )?(FLAT|APARTMENT|MAISONETTE)$', '')
        end,
        ''
    )
"""


# --- stage 1: parts ------------------------------------------------------

ADDRESS_PARTS_SQL = f"""
    {PAON_PATTERN_SQL} as paon_pattern,
    {SAON_PATTERN_SQL} as saon_pattern,
    {IS_FLAT_LIKE_SQL} as is_flat_like,
    {PAON_NUMBER_RAW_SQL} as paon_number_raw,
    {PAON_NAME_RAW_SQL} as paon_name_raw,
    {PAON_LEADING_UNIT_SQL} as paon_leading_unit,
    {PAON_FLAT_RAW_SQL} as paon_flat_raw,
    {PAON_PLOT_RAW_SQL} as paon_plot_raw,
    {PAON_UNIT_TEXT_SQL} as paon_unit_text,
    {SAON_FLAT_NUMBER_SQL} as saon_flat_number,
    {SAON_DESCRIPTION_SQL} as saon_description
"""

# --- stage 2: normalised columns -----------------------------------------

# a bare-number SAON is the house number when the PAON has no number of its
# own and the property is not a flat (PAON 'MILNER COURT' / SAON '2' / type T,
# or PAON '' / SAON '18')
_SAON_IS_BUILDING_NUMBER = f"(saon ~ '{SAON_NUMBER}' and not is_flat_like and paon_number_raw is null and paon !~ '[0-9]')"

# a name that is really a generic unit word ('UNITS', 'GARAGE', 'FLAT') is not a name
_NAME_IS_GENERIC = f"(paon_name_raw ~ '{_GENERIC_UNIT}')"

# (is_flat_like is already a stage 1 column and carries through)
ADDRESS_NORMALISATION_SQL = f"""
    paon_pattern || '/' || saon_pattern as address_pattern,
    {_normalise_number(f"coalesce(paon_number_raw, case when {_SAON_IS_BUILDING_NUMBER} then saon end)")} as building_number,
    case when {_NAME_IS_GENERIC} then null else paon_name_raw end as building_name,
    case
        when is_flat_like then {_normalise_flat("coalesce(saon_flat_number, paon_flat_raw, paon_leading_unit)")}
    end as flat_number,
    case
        when is_flat_like then coalesce(
            saon_description,
            -- a flat-like row whose generic PAON yielded no number ('FLATS 14-18', 'FLAT 1 OLD GATE HOUSE') keeps the text
            case
                when coalesce(saon_flat_number, paon_flat_raw, paon_leading_unit) is null
                    then nullif(regexp_replace(paon_unit_text, '^(THE )?(FLAT|APARTMENT|MAISONETTE)$', ''), '')
            end
        )
    end as flat_description,
    case
        when not is_flat_like then nullif(concat_ws(' / ',
            paon_unit_text,
            case when {_NAME_IS_GENERIC} then paon_name_raw end,
            paon_leading_unit,
            case when not {_SAON_IS_BUILDING_NUMBER} and saon <> '' then saon end
        ), '')
    end as unit_description,
    paon_plot_raw as plot_number
"""

# --- stage 3: key --------------------------------------------------------

# postcode | number-or-name | flat-or-unit. A building number takes
# precedence over a name so that 'MILNER COURT, 9' and '9' on the same
# postcode are the same building
PROPERTY_KEY_NORMALISED_SQL = """
    case
        when postcode is not null
         and coalesce(building_number, building_name, plot_number, flat_number, flat_description, unit_description) is not null then
            postcode
            || '|' || coalesce(building_number, building_name, 'PLOT ' || plot_number, '')
            || '|' || coalesce(flat_number, flat_description, unit_description, '')
    end
"""
