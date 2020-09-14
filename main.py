import geocoder
import asyncio
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
import csv
import pyodbc

QUERY = """select top 200 contactid as code, companyname as company, zip as old_zip, address as old_addr from addrcheck where LAT is null
    and contactid not in ('PA23243', 'PA26728', 'PA27869', 'PA27882', 'PA17771', 'PA20730', 'PA23066', 'PA23329', 'PA25704', 'PA26309', 'PA27228', 'PA94566',
    'PA25841', 'PA26311', 'PA27209', 'PA27849');"""
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


@contextmanager
def mssql_cursor():
    _con = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER=scewhldb.rota.local;PORT=1433;DATABASE=ADDONS;UID=excel;PWD=krnpbmyw;TDS_Version=8.0;")
    _cur = _con.cursor()
    yield _cur
    _cur.close()
    _con.close()


async def query_data(query):
    with mssql_cursor() as _cur:
        _cur.execute(query)
        return {'results':
                    [dict(zip([column[0] for column in _cur.description], row))
                     for row in _cur.fetchall()]}


async def update(query):
    with mssql_cursor() as _cur:
        _cur.execute(query)
        _cur.commit()


async def write_csv(addresses):
    csv_columns = list(addresses[-1].keys())
    with open('test.csv', 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_columns)
        writer.writeheader()
        for _addr in addresses:
            writer.writerow(_addr)


async def geocode(data):
    _res = []
    for _addr in data['results']:
        _r = geocoder.google(_addr['old_addr'], key=KEY)
        _zip = _r.postal
        if not _zip:
            if _r.latlng:
                _reverse_geo = geocoder.reverse(_r.latlng, provider='google', key=KEY)
                _zip = _reverse_geo.postal
            else:
                print('No location for {}, trialto code {}'.format(_r, _addr['code']))
                continue
        _res.append({
            "code": _addr['code'],
            "company": _addr['company'],
            "old_addr": _addr['old_addr'],
            "full_addr": _r.address or "",
            "street": _r.street_long or "",
            "house_number": _r.housenumber or "",
            "zip": _zip or "",
            "city": _r.city_long or "",
            "country": _r.country or "",
            "LatLong": _r.latlng or "",
            "Lat": _r.latlng[0] or "",
            "Lon": _r.latlng[1] or ""
        })
    return _res


async def update_sql(data):
        _row = data
        _res = "update addrcheck set geo_full_addr = '{}', geo_street = '{}', geo_house_number = '{}', geo_zip = '{}', " \
               "geo_city = '{}', geo_country = '{}', lat = {}, lon = {} where contactid = '{}'"\
            .format(_row["full_addr"], _row["street"], _row["house_number"], _row["zip"], _row["city"], _row["country"], _row["Lat"], _row["Lon"], _row["code"])
        return _res


async def reverse_geocode(location):
    _res = []
    for _location in location:
        _r = geocoder.reverse(_location, provider='google', key=KEY)
        _res.append(_r.geojson)
    print(_res)


async def bp():
    _to_resolve = await query_data(QUERY)
    print("To resolve", _to_resolve)
    _addresses = await geocode(_to_resolve)
    print("Resolved", _addresses)

    for _address in _addresses:
        _statement = await update_sql(_address)
        print(_statement)
        await update(_statement)
    #await write_csv(_addresses)


def main():
    _loop = asyncio.get_event_loop()
    _loop.run_until_complete(bp())


if __name__ == '__main__':
    main()
