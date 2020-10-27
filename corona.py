#!pyto://
from datetime import date, datetime, timedelta, tzinfo
import json

import widgets as wd
import requests


def download_arcgis_data(
        base_url,
        out_fields,
        date_fields=None,
        filters=None,
        batch_size=5000,
        params={},
):
    # transform filters to where condition
    if filters is not None:
        where = ""
        for f in filters:
            if len(where) > 0:
                where += " AND "
            if isinstance(f, str):
                where += f
            elif isinstance(f[2], date):
                where += f"{f[0]} {f[1]} DATE '{datetime(f[2].year, f[2].month, f[2].day)}'"
            elif isinstance(f[2], datetime):
                where += f"{f[0]} {f[1]} DATE '{f[2]}'"
            else:
                where += f"{f[0]} {f[1]} '{f[2]}'"
    else:
        where = "1=1"
        
    # transform lists and dicts
    for key in params:
        if isinstance(params[key], (list, dict)):
            params[key] = str(params[key])

    # define query parameter
    params.update({
        "outSR": 4326,
        "returnGeometry": False,
        "f": "json",
        "outFields": ",".join(out_fields),
        "where": where,
    })

    # download all datasets
    data = []
    while len(data) % batch_size == 0:
        response = requests.get(
            base_url,
            params=params, )
        if response.status_code != 200:
            raise requests.ConnectionError
        j = response.json()
        if "error" in j:
            raise ValueError(
                f"Error '{j['error']['message']}' for request {response.url}")
        if len(j["features"]) == 0:
            raise ValueError(f"Empty dataset for request {response.url}")
        data += j["features"]

    # process data
    for ds in data:
        if date_fields is not None:
            for df in date_fields:
                ds["attributes"][df] = datetime.utcfromtimestamp(
                    ds["attributes"][df] / 1000)

    # single dataset responses will be simplified
    if len(data) == 1:
        data = data[0]['attributes']
    
    return data


# download data
rki = download_arcgis_data(
    base_url="https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_COVID19/FeatureServer/0/query",
    out_fields=(
        "AnzahlFall",
    ),
    filters=(
        "NeuerFall IN(1,-1)",
    ),
    params={
        "outStatistics": [{
            "statisticType": "sum",
            "onStatisticField": "AnzahlFall",
            "outStatisticFieldName": "AnzahlFall",
        }],
    },
)

# https://kreispaderborn.hub.arcgis.com/datasets/9020e4c3f15b40a6807cf282504e26f2_4
pb = download_arcgis_data(
    base_url=
    "https://utility.arcgis.com/usrsvcs/servers/9020e4c3f15b40a6807cf282504e26f2/rest/services/secure/KPB_CoronaDashboard_Prod_Secure/MapServer/4/query",
    out_fields=(
        "BE_AKTUELL",
        "BE_VORTAG",
        "ST_AKTUELL",
    ),
    filters=(
        ("GEMEINDE", "=", "(gesamter Kreis)"),
    ),
)
pb["BE_AENDERUNG"] = pb["BE_AKTUELL"] - pb["BE_VORTAG"]

# https://kreispaderborn.hub.arcgis.com/datasets/KreisPaderborn::text-letzte-aktualisierung-1
pb_refresh = download_arcgis_data(
    base_url=
    "https://utility.arcgis.com/usrsvcs/servers/9020e4c3f15b40a6807cf282504e26f2/rest/services/secure/KPB_CoronaDashboard_Prod_Secure/MapServer/5/query",
    out_fields=(
        "DATUM",
    ),
    date_fields=(
        "DATUM",
    ),
)
pb["AKTUALISIERUNG"] = pb_refresh["DATUM"]

# build widgets
widget = wd.Widget()
layout = widget.small_layout
widget.small_layout.add_vertical_spacer()
layout.add_row([
    wd.Text(
        "🦠 COVID-19",
        font=wd.Font.bold_system_font_of_size(wd.FONT_SYSTEM_SIZE),
    ),
])
widget.small_layout.add_vertical_spacer()
layout.add_row([
    wd.Text(
        "Deutschland",
        font=wd.Font.bold_system_font_of_size(wd.FONT_SYSTEM_SIZE),
    ),
])
layout.add_row([
    wd.Text(
        "Neue Fälle:",
    ),
    wd.Text(
        f"{rki['AnzahlFall']}",
    ),
])
widget.small_layout.add_vertical_spacer()
layout.add_row([
    wd.Text(
        "Paderborn",
        font=wd.Font.bold_system_font_of_size(wd.FONT_SYSTEM_SIZE),
    ),
])
layout.add_row([
    wd.Text(
        "Neue Fälle:",
    ),
    wd.Text(
        f"{pb['BE_AENDERUNG']}",
    ),
])
layout.add_row([
    wd.Text(
        "Inzidenz:",
    ),
    wd.Text(
        f"{pb['ST_AKTUELL']:.1f}",
    ),
])
layout.add_row([
    wd.Text(
        f"Aktualisierung: Vor {int((datetime.now() - pb['AKTUALISIERUNG']).total_seconds() / 60 / 60)} h",
        font=wd.Font.system_font_of_size(wd.FONT_SYSTEM_SIZE / 2),
    ),
])
widget.small_layout.add_vertical_spacer()

# Show the widget and reload every 2 hours
wd.schedule_next_reload(60 * 60 * 2)
wd.show_widget(widget)

