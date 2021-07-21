import pandas as pd
from ast import literal_eval
from functools import reduce
from itertools import groupby
import datetime
from dateutil import parser
import re

ADDITIONAL_DATA_FIELDS = ['culture', 'total_amount_raised',
                          'total_amount_raised_2y', 'last_funding_round', 'entity_sites']
# site_name: LinkedIn, Video (Youtube), Blog (multi), Facebook, Instagram, Website, Twitter, CrunchBase, AngelList
# Android App, Wechat, Behance, Github, ITjuzi, Weibo, iOS App, Quora, Projects, Dribbble, Google+
ADDITIONAL_SOCIAL_MEDIA_FIELDS = ['youtube', 'blog', 'facebook', 'instagram', 'twitter', 'angellist', 'android_app',
                                  'wechat', 'behance', 'github', 'itjuzi', 'weibo', 'ios_app', 'quora', 'projects', 'dribbble', 'google+']


def formatAdditionalData(data):
    result = dict.fromkeys(ADDITIONAL_DATA_FIELDS)

    for k in ADDITIONAL_DATA_FIELDS:
        if k == 'entity_sites':
            result.update(data[k])
            continue

        result[k] = str(data[k])
    return result


def map_employee_count_enum(count):
    if not isinstance(count, int):
        return '0'

    if count == 0:
        return '0'

    if count > 0 and count <= 10:
        return '0-10'

    if count >= 11 and count <= 50:
        return '11-50'

    if count >= 51 and count <= 500:
        return '51-500'

    if count >= 501 and count <= 1000:
        return '501-1000'

    if count > 1000:
        return '1000+'


def map_round_enum(round):
    if round == 'LATE STAGE' or round == 'SERIES I' or round == 'SERIES J':
        return 'LATE VC'

    if round == 'BRIDGE' or round == 'UNSPECIFIED STAGE' or round == 'STRATEGIC INVESTMENT':
        return 'VENTURE'

    if round == 'EARLY_STAGE':
        return 'EARLY VC'

    if round == 'M&A':
        return 'MERGER'

    if round == 'POST-IPO FUNDING':
        return 'POST IPO EQUITY'

    if round == 'POST-M&A EQUITY':
        # todo
        pass

    if round == 'POST-IPO EQUITY':
        return 'POST IPO EQUITY'

    if round == 'PRE-SERIES A' or round == 'PRODUCT CROWDFUNDING':
        return 'SEED'

    return round


def convert_hyphen_case_capital_camel_case(data):
    if data is None:
        return ''

    try:
        data_list = data.split('-')

        return ' '.join(list(map(lambda x: x.capitalize(), data_list)))
    except:
        return data


def format_sites_names(name):
    if not isinstance(name, str):
        return ''

    if name == 'Video':
        return 'youtube'

    return name.lower().replace(' ', '_')


def group_entity_sites(data):
    result = dict()
    for key, group in groupby(data, lambda x: format_sites_names(x['site_name'])):
        result[key] = []
        for g in group:
            result[key].append(g['url'].strip('/'))

        result[key] = result[key][0]

    return result


def format_funding_stages(data):
    if len(data) == 0:
        return []

    result = []

    for v in data:
        rounds_inner = v['rounds']
        investors = []

        if len(rounds_inner) == 0:
            continue

        date = rounds_inner[0]['date_ended']

        try:
            year_int = parser.parse(date).year
            month_int = parser.parse(date).month
        except:
            continue

        for r in rounds_inner:
            participants = r['participants']
            investor_slugs = list(map(lambda x: convert_hyphen_case_capital_camel_case(
                x['investor_slug']), participants))

            investors.append(investor_slugs)

        investors_flattern = []

        for ivt in investors:
            investors_flattern += ivt

        mapped_round = map_round_enum(v['stage_name'].upper())

        result.append({
            'amount': v['amount'],
            'year': year_int,
            'month': month_int,
            'currency': 'USD',
            'round': mapped_round,
            'investors': list(set(investors_flattern))
        })

    return result


# default na value is empty string ''
df = pd.read_csv('techinasia.csv', dtype={
    'avatar': 'string',
    'culture': 'string',
    'date_founded': 'string',
    'description': 'string',
    'employee_count': 'string',
    'id': 'string',
    'name': 'string',
    'pitch': 'string',
    'job_posting_count': 'string',
    'total_amount_raised': 'string',
    'total_amount_raised_2y': 'string',
    'last_funding_round': 'string',
    'objectID': 'string',
    'rank': 'string',
    'firstname': 'string',
    'lastname': 'string'
}, index_col=0)

df = df.dropna(subset=['name'])

df = df.drop(columns=['pitch', 'entity_id', 'entity_slug',
                      'objectID', 'rank', 'firstname', 'lastname', 'claimed', 'job_posting_count'])

df = df.fillna('')

# remove Chinese text
df['name'] = df['name'].map(lambda x: re.sub('([^A-Za-z0-9])', '', x).strip())

df['date_founded'] = df['date_founded'].map(
    lambda x: '' if x == '' else parser.parse(x).strftime('%Y-%m-%d'))

df['employee_count'] = df['employee_count'].map(
    lambda x: x.replace(' ', '')).map(
    lambda x: x if x != '' else '0').map(
    lambda x: x if '+' not in x else x.replace('+', '')).map(
    lambda x: x if '-' not in x else str(max(list(map(int, x.split('-')))))).map(
    lambda x: int(x)).map(
    map_employee_count_enum).map(lambda x: [x])

df['employee_count'] += df['date_founded'].map(lambda x: [x])

df['employee_count'] = df['employee_count'].map(
    lambda x: [] if x[0] == '0' or x[1] == '' else [{'range': x[0], 'when': x[1]}])


df['entity_industries'] = df['entity_industries'].map(lambda x: [] if x == '' else literal_eval(
    x)).map(lambda x: [] if len(x) == 0 else list(map(lambda y: y['name'], x)))

df['entity_locations'] = df['entity_locations'].map(
    lambda x: [] if x == '' else literal_eval(x)).map(lambda x: [] if len(x) == 0 else list(map(lambda y: y['country_name'], x))).map(lambda x: '' if len(x) == 0 else x[0])

# entity_sites
# site_name: LinkedIn, Video (Youtube), Blog (multi), Facebook, Instagram, Website, Twitter, CrunchBase, AngelList
# Android App, Wechat, Behance, Github, ITjuzi, Weibo, iOS App, Quora, Projects, Dribbble, Google+
df['entity_sites'] = df['entity_sites'].map(
    lambda x: [] if x == '' else literal_eval(x)).map(group_entity_sites)

df['domain'] = df['entity_sites'].map(
    lambda x: x['website'] if 'website' in x else '')

df['linkedinUrl'] = df['entity_sites'].map(
    lambda x: x['linkedin'] if 'linkedin' in x else '')

df['crunchbaseUrl'] = df['entity_sites'].map(
    lambda x: x['crunchbase'] if 'crunchbase' in x else '')

# df = df.rename({'entity_sites': 'additionalData'})

df['additionalData'] = df.apply(formatAdditionalData, axis=1)
# df['entity_sites'] = df['entity_sites'] + df['rounds']

# funding_stages
# ['Late Stage', 'Series I', 'Bridge', 'Series J', 'Unspecified Stage', 'Early Stage', 'Post-IPO Equity', 'Seed', 'Grant', 'ICO', 'Series E', 'M&A', 'Series B', 'Post-M&A Equity', 'Post-IPO Funding', 'Debt', 'IPO', 'Series F', 'Strategic investment', 'Pre-series A', 'Series C', 'Series H', 'Series D', 'Product Crowdfunding', 'Series G', 'Series A']
df['investors'] = df['funding_stages'].map(lambda x: [] if x == '' else literal_eval(x)).map(
    lambda x: [] if len(x) == 0 else list(map(lambda y: y['rounds'], x))).map(
    # merge arrays into one
    lambda x: [] if len(x) == 0 else reduce((lambda y, z: y+z), x)).map(
    lambda x: [] if len(x) == 0 else list(map(lambda y: y['participants'], x))).map(
    # merge arrays into one
    lambda x: [] if len(x) == 0 else reduce((lambda y, z: y+z), x)).map(
    lambda x: [] if len(x) == 0 else list(map(lambda y: y['investor_slug'], x))).map(
    lambda x: [] if len(x) == 0 else list(filter(lambda y: y is not None, x))).map(
    lambda x: [] if len(x) == 0 else list(map(lambda y: convert_hyphen_case_capital_camel_case(y), x))).map(
    # remove duplicate values
    lambda x: [] if len(x) == 0 else list(set(x)))

df['funding_stages'] = df['funding_stages'].map(
    lambda x: [] if x == '' else literal_eval(x)).map(format_funding_stages)

df['total_amount_raised'] = df['total_amount_raised'].map(
    lambda x: str(int(x)))
df['total_amount_raised_2y'] = df['total_amount_raised_2y'].map(
    lambda x: str(int(x)))


df = df.rename({'avatar': 'logo', 'date_founded': 'founded', 'description': 'about',
                'entity_industries': 'specialities', 'entity_locations': 'country', 'funding_stages': 'rounds', 'employee_count': 'headcounts'}, axis='columns')


df = df.drop(columns=['entity_sites', 'culture', ])


df = df.fillna('')

df.to_csv('./techinasia_formatted.csv', index=False)
