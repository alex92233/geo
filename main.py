import geocoder
import asyncio
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
import csv

QUERY = "select code, company, old_addr from addr a;"
KEY = 'AIzaSyD7bNKeDAq-EhMp2DcLpB9RIiEj0xzpuWE'


@contextmanager
def postgres_cursor():
    _con = psycopg2.connect(dbname='pluto', user='pluto',
                            password='Thursday3!', host='pluto.fixit.lv')
    _cur = _con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        yield _cur
    finally:
        _cur.close()
        _con.close()


async def query_data(query):
    with postgres_cursor() as _cur:
        _cur.execute(query)
        _data = _cur.fetchall()
    return _data


async def write_csv(addresses):
    csv_columns = list(addresses[-1].keys())
    with open('test.csv', 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_columns)
        writer.writeheader()
        for _addr in addresses:
            writer.writerow(_addr)


async def geocode(data):
    _res = []
    for _addr in data:
        _r = geocoder.google(_addr['old_addr'], key=KEY)
        _zip = _r.postal
        if not _zip:
            if _r.latlng:
                _reverse_geo = geocoder.reverse(_r.latlng, provider='google', key=KEY)
                _zip = _reverse_geo.postal
            else:
                print('No location for {}'.format(_r))
        _res.append({
            "code": _addr['code'],
            "company": _addr['company'],
            "old_addr": _addr['old_addr'],
            "full_addr": _r.address,
            "street": _r.street_long,
            "house_number": _r.housenumber,
            "zip": _zip,
            "city": _r.city_long,
            "country": _r.country,
            "LatLong": _r.latlng
        })
        print(_res)
    return _res


async def reverse_geocode(location):
    _res = []
    for _location in location:
        _r = geocoder.reverse(_location, provider='google', key=KEY)
        _res.append(_r.geojson)
    print(_res)


async def bp():
    _to_resolve = await query_data(QUERY)
    _addresses = await geocode(_to_resolve)
    await write_csv(_addresses)


def main():
    _loop = asyncio.get_event_loop()
    _loop.run_until_complete(bp())


if __name__ == '__main__':
    main()
