import http.client
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from os import getenv
from pathlib import Path
from sys import argv

THIS_FILE = Path(__file__)
THIS_DIR = THIS_FILE.parent
BASE_URL = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze"
LAYER_NAME_TEMPLATE = "SkorowidzOrtofomapy{year}"
MESSAGE_TEMPLATE = """
Wygląda na to, że dodano nowe arkusze ortofotomapy do pobrania z Geoportalu z datami między: {old_date} i {new_date}.

Liczba nowych arkuszy: {number_matched}
Warstwa: {layer}
[WFS URL]({url})
"""


def get_wfs_params(layer: str, lower_bound: str, upper_bound: str) -> dict[str, str]:
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


def make_request(url: str, retries: int = 3, timeout: timedelta = timedelta(minutes=5)) -> ET.Element:
    attempt = 0
    while attempt < retries:
        try:
            print(f"Making request to: {url}")
            with urllib.request.urlopen(url, timeout=timeout.total_seconds()) as response:
                if response.status != 200:
                    raise Exception(f"Request failed with status code: {response.status}. Text: {response.read()}")
                
                # Read and parse the XML
                xml_data = response.read()
                parsed_xml = ET.fromstring(xml_data)
                
                return parsed_xml

        except urllib.error.URLError as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            attempt += 1
            time.sleep(1)

    raise Exception(f"Failed to retrieve data after {retries} attempts.")


def get_number_matched_from_response(el: ET.Element) -> int | None:
    if el.tag != "{http://www.opengis.net/wfs/2.0}FeatureCollection":
        raise ValueError(f"Expected tag 'wfs:FeatureCollection' got: {el.tag}")
    number_matched = el.attrib.get("numberMatched")
    if number_matched == "unknown":
        number_matched = el.attrib.get("numberReturned")
    if number_matched:
        return int(number_matched)
    else:
        return None


def get_max_date_from_response(el: ET.Element) -> str:
    def get_dates():
        for element in el.findall(".//{http://www.gugik.gov.pl}dt_pzgik"):
            tp = element.find("{http://www.opengis.net/gml/3.2}timePosition")
            assert tp is not None
            assert tp.text is not None
            yield tp.text
    return max(get_dates())


def post_to_discord(webhook_url: str, message: str):
    # Parse the webhook URL to get components
    parsed_url = urllib.parse.urlparse(webhook_url)

    if len(message) > 2000:
        print("Message length over 2000. Truncating...")
        message = message[:2000]

    # Prepare the JSON payload
    payload = json.dumps({"content": message})
    
    # Set up the connection to the Discord server
    connection = http.client.HTTPSConnection(parsed_url.netloc)
    
    # Set the headers
    headers = {
        "Content-Type": "application/json",
        "Content-Length": str(len(payload))
    }
    
    # Make the POST request to the Discord webhook
    try:
        connection.request("POST", parsed_url.path, body=payload, headers=headers)
        response = connection.getresponse()
        response_data = response.read().decode()
        
        # Check if the status code is 204, which means success with no content
        if response.status != 204:
            raise Exception(f"Request failed with status code: {response.status} and response: {response_data}")
        
        return response_data
    except Exception as e:
        raise Exception(f"Failed to send POST request: {e}")
    finally:
        connection.close()


def main(date_var: date, layer: str, webhook_url: str, state_file: Path) -> None:
    print(f"Processing for date: {date_var}")
    date_str = date_var.isoformat()
    request_params = get_wfs_params(
        layer=layer,
        lower_bound=date_str,
        upper_bound=(date.today() - timedelta(days=1)).isoformat(),
    )
    print(f"Request params: {request_params}")
    url = f"{BASE_URL}?{urllib.parse.urlencode(request_params)}"
    result = make_request(url=url)
    number_matched = get_number_matched_from_response(el=result)
    if number_matched:
        new_date_str = get_max_date_from_response(el=result)
        params_with_update_upper_bound = get_wfs_params(
            layer=layer,
            lower_bound=date_str,
            upper_bound=new_date_str,
        )
        message = MESSAGE_TEMPLATE.format(
            number_matched=number_matched,
            layer=layer,
            url=f"{BASE_URL}?{urllib.parse.urlencode(params_with_update_upper_bound)}",
            old_date=date_var.isoformat(),
            new_date=new_date_str,
        )
        print(f"Posting message to discord: {message}")
        post_to_discord(webhook_url=webhook_url, message=message)
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
