"""Decompose time series API URL."""

ALLOWED_DOMAINS = (
    'ru',
    'oil',
    'all',
    # some else?
)

ALLOWED_FREQUENCIES = 'dwmqa'

ALLOWED_REAL_RATES = (
    'rog',
    'yoy',
    'base'
)
ALLOWED_AGGREGATORS = (
    'eop',
    'avg'
)
ALLOWED_FINALISERS = (
    'info',
    'csv',
    'json',  # should be default
    'xlsx'
)


def get_years(tokens):
    """Extract years from a list of *tokens* strings."""
    start, end = None, None
    integers = [x for x in tokens if x.isdigit()]
    if len(integers) == 1:
        start = integers[0]
    elif len(integers) == 2:
        start = integers[0]
        end = integers[1]
    return start, end


def assign_values(tokens, allowed_values):
    """Find entries of *allowed_values* into *tokens*."""
    values_found = [p for p in allowed_values if p in tokens]
    if not values_found:
        return None
    elif len(values_found) == 1:
        return values_found[0]
    else:
        raise ValueError(values_found)


def decompose_inner_path(inner_path: str):
    """Return dictionary with custom API parameters from *inner_path*"""
    tokens = [token.strip() for token in inner_path.split('/') if token]
    return decompose_inner_tokens(tokens)


def decompose_inner_tokens(tokens):
    start, end = get_years(tokens)
    return dict(rate=assign_values(tokens, ALLOWED_REAL_RATES),
                agg=assign_values(tokens, ALLOWED_AGGREGATORS),
                fin=assign_values(tokens, ALLOWED_FINALISERS),
                start=start,
                end=end)


def validate_freq(freq: str):
    return assign_values([freq], ALLOWED_FREQUENCIES)

# NOT USED NOW:
#BASE_URL = 'api/series/{string:domain}/{string:varname}/{string:freq}'
#@ts.route(f'{BASE_URL}')
#@ts.route(f'{BASE_URL}/<path:inner_path>')


def time_series_api_interface(domain, varname, freq, inner_path=None):
    """Decompose incoming URL into API request."""

    # FIXME: must use validate_freq() here
    if freq not in 'dwmqa':
        return jsonify({
            'error': "Frequency value is invalid"
        }), 400
    # ---------------
    ctx = {
        'domain': domain,
        'varname': varname,
        'frequency': freq,
        'rate': None,
        'agg': None,
        'start': None,
        'end': None
    }
    if inner_path is not None:
        optional_args = decompose_inner_path(inner_path)
        ctx.update(**optional_args)
    return jsonify(ctx)
# END ---


# decode path like 'api/oil/series/BRENT/m/eop/2015/2017/csv'
def mimic_custom_api(path: str):
    assert path.startswith('api/')
    tokens = [token.strip() for token in path.split('/') if token]
    ctx = dict(domain=tokens[1],
               varname=tokens[3],
               freq=tokens[4])
    ctx.update(**decompose_inner_tokens(tokens))
    return ctx

if __name__ == "__main__":   
    test_pairs = {                       
        'api/oil/series/BRENT/m/eop/2015/2017/csv':                   
        {'domain': 'oil',
         'varname': 'BRENT',
         'freq': 'm',
         'rate': None,
         'start': '2015',
         'end': '2017',
         'agg': 'eop',
         'fin': 'csv'
         }
    }
        
    for url, d in test_pairs.items():
        assert mimic_custom_api(url) == d    
