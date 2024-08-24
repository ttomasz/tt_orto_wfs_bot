# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests<3",
#     "geoplot==0.5.1",
# ]
# ///
import json
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from os import getenv
from pathlib import Path
from sys import argv
from tempfile import NamedTemporaryFile
from typing import (IO, BinaryIO, Generator, Iterable, NamedTuple, Optional,
                    TextIO)

import geopandas as gpd
import geoplot as gplt
import geoplot.crs as gcrs
import matplotlib.pyplot as plt
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

THIS_FILE = Path(__file__)
THIS_DIR = THIS_FILE.parent
BASE_URL = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze"
LAYER_NAME_TEMPLATE = "SkorowidzOrtofomapy{year}"
MESSAGE_TEMPLATE = """
Wygląda na to, że dodano nowe arkusze ortofotomapy do pobrania z Geoportalu z datami dodania do rejestru między: {old_date} i {new_date}.

Liczba nowych arkuszy: {number_matched}
Warstwa: {layer}

[WFS URL]({url})

Załączony plik z zasięgami zobrazowań. Po pobraniu możesz go wyświetlić na przykład na stronie: [geojson.io](https://geojson.io)
"""


class Envelope(NamedTuple):
    xmin: float
    ymin: float
    xmax: float
    ymax: float


def _feature(e: Envelope, properties: Optional[dict]) -> dict:
    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    (e.xmin, e.ymin),
                    (e.xmin, e.ymax),
                    (e.xmax, e.ymax),
                    (e.xmax, e.ymin),
                    (e.xmin, e.ymin),
                ],
            ],
        },
        "properties": properties or dict(),
    }


def _feature_collection(features: Iterable[dict], bbox: Optional[Envelope] = None) -> dict:
    d = {
        "type": "FeatureCollection",
        "features": [f for f in features],
    }
    if bbox is not None:
        d["bbox"] = (bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax)
    return d


def _get_envelope_from_element(el: ET.Element) -> Envelope:
    envelope = el.find(".//{http://www.opengis.net/gml/3.2}Envelope")
    assert envelope is not None
    lower_corner = envelope.find("./{http://www.opengis.net/gml/3.2}lowerCorner")
    upper_corner = envelope.find("./{http://www.opengis.net/gml/3.2}upperCorner")
    assert lower_corner is not None and lower_corner.text is not None
    assert upper_corner is not None and upper_corner.text is not None
    ymin, xmin = lower_corner.text.split(" ")
    ymax, xmax = upper_corner.text.split(" ")
    return Envelope(
        xmin=float(xmin),
        ymin=float(ymin),
        xmax=float(xmax),
        ymax=float(ymax),
    )


def _get_chosen_attributes_from_element(el: ET.Element) -> dict:
    akt_data = el.find(".//{http://www.gugik.gov.pl}akt_data")
    akt_data = akt_data if akt_data is not None else ET.Element("emptyElement")
    akt_data = akt_data.find("./{http://www.opengis.net/gml/3.2}timePosition")
    dt_pzgik = el.find(".//{http://www.gugik.gov.pl}dt_pzgik")
    dt_pzgik = dt_pzgik if dt_pzgik is not None else ET.Element("emptyElement")
    dt_pzgik = dt_pzgik.find("./{http://www.opengis.net/gml/3.2}timePosition")
    url = el.find(".//{http://www.gugik.gov.pl}url_do_pobrania")
    return {
        "akt_data": akt_data.text if akt_data is not None else None,
        "dt_pzgik": dt_pzgik.text if dt_pzgik is not None else None,
        "url": url.text if url is not None else None,
    }


def _get_features_from_response(el: ET.Element) -> Generator[dict, None, None]:
    members = el.findall(".//{http://www.opengis.net/wfs/2.0}member")
    for member in members:
        envelope = _get_envelope_from_element(el=member)
        attributes = _get_chosen_attributes_from_element(el=member)
        yield _feature(e=envelope, properties=attributes)


def convert_response_to_geojson(parsed_xml: ET.Element) -> dict:
    print("Converting response to geojson...")
    bounded_by = parsed_xml.find(".//{http://www.opengis.net/wfs/2.0}boundedBy")
    assert bounded_by is not None
    bbox = _get_envelope_from_element(el=bounded_by)
    geojson = _feature_collection(
        features=_get_features_from_response(el=parsed_xml),
        bbox=bbox,
    )
    print("Finished converting.")
    return geojson


def generate_plot(geojson_fp: TextIO, output_fp: BinaryIO, title: str) -> None:
    print("Generating plot...")
    gdf = gpd.read_file(geojson_fp)
    ax = gplt.webmap(gdf, projection=gcrs.WebMercator())
    ax.set_title(title, fontsize=16)
    attribution_text = "Map © OpenStreetMap contributors"
    plt.figtext(0.5, 0.08, attribution_text, ha="center", fontsize=8)
    gplt.polyplot(gdf, ax=ax)
    plt.savefig(output_fp, dpi=300, bbox_inches="tight", format="png")
    print("Finished generating plot.")


def _get_wfs_params(layer: str, lower_bound: str, upper_bound: str) -> dict[str, str]:
    return dict(
        SERVICE="WFS",
        REQUEST="GetFeature",
        VERSION="2.0.0",
        outputFormat="text/xml; subtype=gml/3.2.1",
        SRSNAME="EPSG:4326",
        TYPENAME=layer,
        Filter=f"""
<Filter>
  <PropertyIsGreaterThan>
    <PropertyName>dt_pzgik</PropertyName>
    <LowerBoundary><Literal>{lower_bound}</Literal></LowerBoundary>
    <UpperBoundary><Literal>{upper_bound}</Literal></UpperBoundary>
  </PropertyIsGreaterThan>
</Filter>""".strip(),
    )


def make_request(url: str, params: dict, retries: int = 3, timeout: timedelta = timedelta(minutes=5)) -> ET.Element:
    # Configure retries
    retry_strategy = Retry(
        total=retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=1  # wait time between retries: 2nd try waits 1s, 3rd waits 2s, etc.
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    print(f"Making request to: {url} with params: {params}...")
    response = session.get(url, params=params, timeout=timeout.total_seconds())
    print("Received response.")    
    response.raise_for_status()
    parsed_xml = ET.fromstring(response.content)
    return parsed_xml


def _get_number_matched_from_response(el: ET.Element) -> int | None:
    if el.tag != "{http://www.opengis.net/wfs/2.0}FeatureCollection":
        raise ValueError(f"Expected tag 'wfs:FeatureCollection' got: {el.tag}")
    number_matched = el.attrib.get("numberMatched")
    if number_matched == "unknown":
        number_matched = el.attrib.get("numberReturned")
    if number_matched:
        return int(number_matched)
    else:
        return None


def _get_max_date_from_response(el: ET.Element) -> str:
    def get_dates():
        for element in el.findall(".//{http://www.gugik.gov.pl}dt_pzgik"):
            tp = element.find("{http://www.opengis.net/gml/3.2}timePosition")
            assert tp is not None
            assert tp.text is not None
            yield tp.text
    return max(get_dates())


def post_to_discord(webhook_url: str, message: str, files: Optional[dict[str, tuple[str, IO, str]]] = None) -> None:
    if len(message) > 2000:
        print("Message length over 2000. Truncating...")
        message = message[:2000]
    payload = {"content": message, "flags": 1 << 2}  # flag suppressing embeds
    print("Sending message to discord...")
    response = requests.post(webhook_url, data=payload, files=files)
    response.raise_for_status()
    print("Message sent.")


def main(date_var: date, layer: str, webhook_url: str, state_file: Path) -> None:
    print(f"Processing for date: {date_var}")
    date_str = date_var.isoformat()
    request_params = _get_wfs_params(
        layer=layer,
        lower_bound=date_str,
        upper_bound=(date.today() - timedelta(days=1)).isoformat(),
    )
    print(f"Request params: {request_params}")
    # url = f"{BASE_URL}?{urllib.parse.urlencode(request_params)}"
    result = make_request(url=BASE_URL, params=request_params)
    number_matched = _get_number_matched_from_response(el=result)
    if number_matched:
        new_date_str = _get_max_date_from_response(el=result)
        params_with_update_upper_bound = _get_wfs_params(
            layer=layer,
            lower_bound=date_str,
            upper_bound=new_date_str,
        )
        message = MESSAGE_TEMPLATE.format(
            number_matched=number_matched,
            layer=layer,
            url=f"{BASE_URL}?{urllib.parse.urlencode(params_with_update_upper_bound)}",
            old_date=date_str,
            new_date=new_date_str,
        )
        geojson = convert_response_to_geojson(parsed_xml=result)
        with NamedTemporaryFile("r+") as geojson_fp, NamedTemporaryFile("rb+") as plot_fp:
            json.dump(geojson, geojson_fp)
            geojson_fp.seek(0)  # move pointer back to beginning of file so we can read what we just wrote
            generate_plot(geojson_fp=geojson_fp, output_fp=plot_fp, title=f"Ortofotomapy dodane między {date_str} a {new_date_str}")
            plot_fp.seek(0)
            print(f"Posting message to discord: {message}")
            post_to_discord(
                webhook_url=webhook_url,
                message=message,
                files={
                    "file": (f"zasiegi_{date_str}_{new_date_str}.geojson", geojson_fp, "application/geo+json"),
                    "image": (f"zasiegi_{date_str}_{new_date_str}.png", plot_fp, "image/png"),
                },
            )
        print(f"Updating {state_file.name} file with value: {new_date_str}")
        state_file.write_text(new_date_str, encoding="utf-8")
    else:
        print("Nothing to do")


def parse_date_from(path: Path) -> date | None:
    if path.is_file():
        date_str = path.read_text().strip()
        return date.fromisoformat(date_str)
    else:
        return None


if __name__ == "__main__":
    yesterday = date.today() - timedelta(days=1)
    if len(argv) == 2:
        date_used = date.fromisoformat(argv[1])
    else:
        date_used = yesterday
    current_year = date_used.year
    previous_year = current_year - 1
    print(f"Current year: {current_year}, previous year: {previous_year}.")

    try:
        import dotenv
        print("Trying to load .env file")
        dotenv.load_dotenv()
    except ImportError:
        pass
    webhook_url = getenv("WEBHOOK_URL")
    if not webhook_url:
        raise Exception("Missing env variable: WEBHOOK_URL")

    print("Processing previous year layer")
    previous_year_layer = LAYER_NAME_TEMPLATE.format(year=previous_year)
    previous_year_file = THIS_DIR / f"last_date_{previous_year}.txt"
    previous_year_date_used = parse_date_from(path=previous_year_file) or yesterday
    main(date_var=previous_year_date_used, layer=previous_year_layer, webhook_url=webhook_url, state_file=previous_year_file)

    print("Processing current year layer")
    current_year_layer = LAYER_NAME_TEMPLATE.format(year=current_year)
    current_year_file = THIS_DIR / f"last_date_{current_year}.txt"
    current_year_date_used = parse_date_from(path=current_year_file) or yesterday
    main(date_var=current_year_date_used, layer=current_year_layer, webhook_url=webhook_url, state_file=current_year_file)

    print("Done")
