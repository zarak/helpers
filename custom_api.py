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
    s, e = get_years(tokens)
    return dict(rate  = assign_values(tokens, ALLOWED_REAL_RATES),
                agg   = assign_values(tokens, ALLOWED_AGGREGATORS),
                fin   = assign_values(tokens, ALLOWED_FINALISERS),
                start = s,
                end   = e)


        
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


def error_catcher(path: str):
    ctx = mimic_custom_api(path)
    if not is_valid_frequency(ctx):
        return dict(error=f"Frequency <{d['freq']}> is not valid")
    if has_double_suffix(ctx):
        return dict(error=f"Cannot combine <{d['agg']}> and <{d['rate']}>")
    return ctx

def has_double_suffix(d):
    return d['agg'] and d['rate']


def is_valid_frequency(d):
    return d['freq'] in ALLOWED_FREQUENCIES


if __name__ == "__main__":
    import pytest
    
    # api/{domain}/series/{varname}/{freq}/{?suffix}/{?start}/{?end}/{?finaliser}
    # {?rate}/{?agg} are mutually exclusive, we can either have {?rate} or {?agg}, so better call them {?suffix}
    # if {?suffix} is in (eop, avg) then {agg} is defined
    # if {?suffix} is in (yoy, rog) then {rate} is defined
    
    test_pairs = {
        'api/oil/series/BRENT/m/eop/2015/2017/csv': {
            'domain': 'oil',
            'varname': 'BRENT',
            'freq': 'm',
            'rate': None,
            'start': '2015',
            'end': '2017',
            'agg': 'eop',
            'fin': 'csv'
        },
        'api/ru/series/USDRUR/m/avg/2017': {
            'domain': 'ru',
            'varname': 'USDRUR',
            'freq': 'm',
            'rate': None,
            'agg': 'avg',
            'fin': None,
            'start': '2017',
            'end': None
        },
                
        # no aggregator, base frequency       
        'api/ru/series/USDRUR/d/xlsx': {
            'domain': 'ru',
            'varname': 'USDRUR',
            'freq': 'd',
            'rate': None,
            'agg': None,
            'fin': 'xlsx',
            'start': None,
            'end': None
        },
                
        # info should give details about time series, but not data        
        'api/ru/series/INDPRO/a/yoy/2013/2015/info': {
            'domain': 'ru',
            'varname': 'INDPRO',
            'freq': 'a',
            'rate': 'yoy',
            'agg': None,
            'fin': 'info',
            'start': '2013',
            'end': '2015'
        },
                
        # on next level this should be an error as 
        # prices do not have 'rog' rate in database
        'api/oil/series/BRENT/q/rog': {
            'domain': 'oil',
            'varname': 'BRENT',
            'freq': 'q',
            'rate': 'rog',
            'agg': None,
            'fin': None,
            'start': None,
            'end': None
        }
    }
        
    for url, d in test_pairs.items():
        print (url, 'translates to', d)
        assert mimic_custom_api(url) == d

    #examples that fail error_catcher()
    e1 = error_catcher('api/oil/series/BRENT/q/rog/eop')
    e2 = error_catcher('api/oil/series/BRENT/z/')
    assert 'error' in e1.keys() 
    assert 'error' in e2.keys()
    
    #TODO:
    #    translate valid custom API call to db GET method call
     