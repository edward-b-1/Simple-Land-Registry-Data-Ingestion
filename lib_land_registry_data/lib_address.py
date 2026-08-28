
# SQL for normalising the Price Paid address fields PAON (primary addressable
# object name) and SAON (secondary addressable object name) into separate
# columns. The raw fields mix several conventions, e.g.
#
#   PAON '42'                 SAON ''          -> building_number 42
#   PAON '12A'                SAON 'FLAT 3'    -> building_number 12A, flat_number 3
#   PAON 'MILNER COURT, 9'    SAON 'FLAT 1'    -> building_name MILNER COURT, building_number 9, flat_number 1
#   PAON 'MILNER COURT'       SAON '2' (house) -> building_name MILNER COURT, building_number 2
#   PAON 'MILNER COURT'       SAON '2' (flat)  -> building_name MILNER COURT, flat_number 2
#   PAON 'FAIRHAVEN'          SAON ''          -> building_name FAIRHAVEN
#   PAON '4 MANOR FARM BARNS' SAON ''          -> building_number 4, building_name MANOR FARM BARNS
#   PAON 'FLAT 1, 33'         SAON ''          -> building_number 33, flat_number 1
#   PAON 'PLOT 4'             SAON ''          -> plot_number 4
#   PAON 'ALBERT HOUSE, 192'  SAON 'FIRST FLOOR FLAT' -> ..., flat_description FIRST FLOOR FLAT
#
# `flat_number` / `flat_description` are only populated for property_type F;
# sub-units of other property types (UNIT 4, GARAGE 2, a stray 'FLAT 1' on a
# terraced house) go to `unit_description` unchanged. `address_pattern`
# records which PAON and SAON rule fired (e.g. 'P_NAME_NUMBER/S_FLAT') so the
# rules can be audited and refined.
#
# The fragments are written against column aliases `paon`, `saon` and
# `property_type` and are meant to be spliced into a larger query (see
# main_pp_transactions.py). They contain no '%' characters, so they are safe
# inside a psycopg query that uses %(name)s parameters.

# --- PAON ---------------------------------------------------------------

_NUMBER = r"[0-9]+[A-Z]?"
_RANGE = rf"{_NUMBER} *(-|/|TO|&|AND) *{_NUMBER}"
_NUMBER_OR_RANGE = rf"{_NUMBER}( *(-|/|TO|&|AND) *{_NUMBER})?"

PAON_NUMBER = rf"^{_NUMBER}$"
PAON_RANGE = rf"^{_RANGE}$"
PAON_NAME_NUMBER = rf"^.+, *{_NUMBER_OR_RANGE}$"          # 'MILNER COURT, 9'
PAON_NUMBER_NAME = rf"^{_NUMBER_OR_RANGE} +[A-Z].*$"      # '4 MANOR FARM BARNS'
PAON_NAME_NUMBER_NOCOMMA = rf"^.+[A-Z] +{_RANGE}$"        # 'RIVERSIDE LOFTS 35-36'
PAON_FLAT = rf"^(FLAT|APARTMENT|APT) +[0-9A-Z.]+(, *{_NUMBER_OR_RANGE})?$"
PAON_PLOT = r"^PLOTS? +.+$"

PAON_PATTERN_SQL = f"""
    case
        when paon = '' then 'P_EMPTY'
        when paon ~ '{PAON_NUMBER}' then 'P_NUMBER'
        when paon ~ '{PAON_RANGE}' then 'P_RANGE'
        when paon ~ '{PAON_NAME_NUMBER}' then 'P_NAME_NUMBER'
        when paon ~ '{PAON_FLAT}' then 'P_FLAT'
        when paon ~ '{PAON_PLOT}' then 'P_PLOT'
        when paon ~ '{PAON_NUMBER_NAME}' then 'P_NUMBER_NAME'
        when paon ~ '{PAON_NAME_NUMBER_NOCOMMA}' then 'P_NAME_RANGE'
        when paon ~ '[0-9]' then 'P_OTHER'
        else 'P_NAME'
    end
"""

# normalise '54 - 56' / '54 TO 56' / '54/56' to '54-56'
def _normalise_range(expression: str) -> str:
    return f"regexp_replace(regexp_replace({expression}, ' *(-|/|TO|&|AND) *', '-'), ' ', '', 'g')"


_PAON_TRAILING_NUMBER = rf", *({_NUMBER_OR_RANGE})$"           # the ', 9' of 'MILNER COURT, 9'
_PAON_LEADING_NUMBER = rf"^({_NUMBER_OR_RANGE}) +[A-Z]"        # the '4 ' of '4 MANOR FARM BARNS'
_PAON_TRAILING_RANGE = rf"[A-Z] +({_RANGE})$"                  # the '35-36' of 'RIVERSIDE LOFTS 35-36'

PAON_BUILDING_NUMBER_SQL = f"""
    case
        when paon ~ '{PAON_NUMBER}' then paon
        when paon ~ '{PAON_RANGE}' then {_normalise_range('paon')}
        when paon ~ '{PAON_NAME_NUMBER}' then {_normalise_range(f"substring(paon from '{_PAON_TRAILING_NUMBER}')")}
        when paon ~ '{PAON_FLAT}' and paon ~ ',' then {_normalise_range(f"substring(paon from '{_PAON_TRAILING_NUMBER}')")}
        when paon ~ '{PAON_PLOT}' then null
        when paon ~ '{PAON_NUMBER_NAME}' then {_normalise_range(f"substring(paon from '{_PAON_LEADING_NUMBER}')")}
        when paon ~ '{PAON_NAME_NUMBER_NOCOMMA}' then {_normalise_range(f"substring(paon from '{_PAON_TRAILING_RANGE}')")}
    end
"""

PAON_BUILDING_NAME_SQL = f"""
    case
        when paon = '' then null
        when paon ~ '{PAON_NUMBER}' or paon ~ '{PAON_RANGE}' or paon ~ '{PAON_FLAT}' then null
        when paon ~ '{PAON_NAME_NUMBER}' then btrim(regexp_replace(paon, '{_PAON_TRAILING_NUMBER}', ''))
        when paon ~ '{PAON_PLOT}' then nullif(btrim(substring(paon from '^PLOTS? +[0-9A-Z]+(?: *(?:-|/|TO|&|AND) *[0-9A-Z]+)? +(.+)$')), '')
        when paon ~ '{PAON_NUMBER_NAME}' then btrim(regexp_replace(paon, '^{_NUMBER_OR_RANGE} +', ''))
        when paon ~ '{PAON_NAME_NUMBER_NOCOMMA}' then btrim(regexp_replace(paon, ' +{_RANGE}$', ''))
        else paon
    end
"""

# 'PLOT 4' -> 4, 'PLOTS 1-8' -> 1-8, 'PLOTS H1 TO H3' -> H1-H3
PAON_PLOT_NUMBER_SQL = f"""
    case
        when paon ~ '{PAON_PLOT}' then {_normalise_range("substring(paon from '^PLOTS? +([0-9A-Z]+(?: *(?:-|/|TO|&|AND) *[0-9A-Z]+)?)')")}
    end
"""

# a flat number given in the PAON ('FLAT 1, 33' or 'FLAT D4')
PAON_FLAT_NUMBER_SQL = f"""
    case
        when paon ~ '{PAON_FLAT}' then substring(paon from '^(?:FLAT|APARTMENT|APT) +([0-9A-Z.]+)')
    end
"""

# --- SAON ---------------------------------------------------------------

# non-capturing so that substring() returns the number group that follows it
_FLAT_WORD = r"(?:FLAT|FLATS|APARTMENT|APARTMENTS|APARTMANT|APPARTMENT|APT|UNIT|PENTHOUSE|MAISONETTE|STUDIO)"

# a flat identifier: contains a digit ('3', '14A', 'G.03', 'A12') or is a single letter ('D')
_FLAT_ID = r"(?:[A-Z]?[0-9]+[0-9A-Z.]*|[A-Z][0-9.]*[0-9][0-9A-Z.]*|[A-Z])"
SAON_FLAT = rf"^{_FLAT_WORD} +{_FLAT_ID}$"                     # 'FLAT 3', 'APARTMENT 14', 'FLAT D', 'UNIT G.03'
SAON_FLAT_LOOSE = rf"{_FLAT_WORD}\.? *([0-9]+[A-Z]?(\.[0-9]+)?)"  # 'SECOND FLOOR FLAT 33', 'FLAT 2 ARLINGTON COURT'
SAON_NUMBER = rf"^{_NUMBER}$"
SAON_LETTER = r"^[A-Z]$"
SAON_UNIT = r"^UNITS? +.+$"
SAON_FLOOR = r"(FLOOR|BASEMENT|GROUND|MAISONETTE|ANNEX|PENTHOUSE|GARDEN|TOP|UPPER|LOWER|REAR|FRONT|SIDE)"

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

# a bare-number SAON is the house number when the PAON is a name and the
# property is not a flat (PAON 'MILNER COURT', SAON '2', type T)
SAON_IS_BUILDING_NUMBER_SQL = f"""
    (saon ~ '{SAON_NUMBER}' and property_type <> 'F' and paon !~ '[0-9]' and paon <> '')
"""

# the flat identifier taken from the SAON, only for flats
SAON_FLAT_NUMBER_SQL = f"""
    case
        when property_type <> 'F' then null
        when saon ~ '{SAON_FLAT}' then substring(saon from '^{_FLAT_WORD} +({_FLAT_ID})$')
        when saon ~ '{SAON_NUMBER}' or saon ~ '{SAON_LETTER}' then saon
        when saon ~ '{SAON_FLAT_LOOSE}' then substring(saon from '{SAON_FLAT_LOOSE}')
    end
"""

# descriptive SAON on a flat ('FIRST FLOOR FLAT', 'GARDEN FLAT', 'FLAT 2 ARLINGTON COURT');
# kept even when a flat number could be extracted from it
SAON_FLAT_DESCRIPTION_SQL = f"""
    case
        when property_type = 'F' and saon <> '' and saon !~ '{SAON_FLAT}' and saon !~ '{SAON_NUMBER}' and saon !~ '{SAON_LETTER}' then saon
    end
"""

# any SAON on a non-flat that is not the house number ('UNIT 4', 'GARAGE 2',
# a stray 'FLAT 1' on a house)
SAON_UNIT_DESCRIPTION_SQL = f"""
    case
        when property_type <> 'F' and saon <> '' and not {SAON_IS_BUILDING_NUMBER_SQL} then saon
    end
"""


# --- combined -----------------------------------------------------------

# columns produced: address_pattern, building_number, building_name,
# flat_number, flat_description, unit_description, plot_number
ADDRESS_NORMALISATION_SQL = f"""
    ({PAON_PATTERN_SQL}) || '/' || ({SAON_PATTERN_SQL}) as address_pattern,
    coalesce(
        {PAON_BUILDING_NUMBER_SQL},
        case when {SAON_IS_BUILDING_NUMBER_SQL} then saon end
    ) as building_number,
    {PAON_BUILDING_NAME_SQL} as building_name,
    case
        when property_type = 'F' then coalesce({SAON_FLAT_NUMBER_SQL}, {PAON_FLAT_NUMBER_SQL})
    end as flat_number,
    {SAON_FLAT_DESCRIPTION_SQL} as flat_description,
    {SAON_UNIT_DESCRIPTION_SQL} as unit_description,
    {PAON_PLOT_NUMBER_SQL} as plot_number
"""

# the normalised property key, built from the columns above (aliases must be
# in scope): postcode | number-or-name | flat-or-unit. A building number
# takes precedence over a name so that 'MILNER COURT, 9' and '9' on the
# same postcode are the same building
PROPERTY_KEY_NORMALISED_SQL = """
    case
        when postcode is not null
         and coalesce(building_number, building_name, plot_number, flat_number) is not null then
            postcode
            || '|' || coalesce(building_number, building_name, 'PLOT ' || plot_number, '')
            || '|' || coalesce(flat_number, flat_description, unit_description, '')
    end
"""
