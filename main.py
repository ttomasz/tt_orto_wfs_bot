import http.client
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from os import getenv
from sys import argv

BASE_URL = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze"
LAYER_NAME_TEMPLATE = "SkorowidzOrtofomapy{year}"
MESSAGE_TEMPLATE = """
Wygląda na to, że dodano nowe arkusze ortofotomapy do pobrania z Geoportalu dla daty: {date}.

Liczba nowych arkuszy: {number_matched}
Warstwa: {layer}
[WFS URL]({url})
"""

def get_wfs_params(layer: str, date_var: date) -> dict[str, str]:
    return dict(
        SERVICE="WFS",
        REQUEST="GetFeature",
        VERSION="2.0.0",
        outputFormat="text/xml; subtype=gml/3.2.1",
        SRSNAME="EPSG:4326",
        TYPENAME=layer,
        Filter=f"""
<Filter>
  <PropertyIsEqualTo>
    <PropertyName>dt_pzgik</PropertyName>
    <Literal>{date_var.isoformat()}</Literal>
  </PropertyIsEqualTo>
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


def main(date_var: date, layer: str, webhook_url: str) -> None:
    request_params = get_wfs_params(layer=layer, date_var=date_var)
    print(f"Request params: {request_params}")
    url = f"{BASE_URL}?{urllib.parse.urlencode(request_params)}"
    result = make_request(url=url)
    number_matched = get_number_matched_from_response(el=result)
    if number_matched:
        message = MESSAGE_TEMPLATE.format(number_matched=number_matched, layer=layer, url=url, date=date_var)
        print(f"Posting message to discord: {message}")
        post_to_discord(webhook_url=webhook_url, message=message)
    else:
        print("Nothing to do")


if __name__ == "__main__":
    if len(argv) == 2:
        date_used = date.fromisoformat(argv[1])
    else:
        date_used = date.today() - timedelta(days=1)
    print(f"Date used: {date_used}")
    current_year = date_used.year
    previous_year = current_year - 1

    try:
        import dotenv
        print("Trying to load .env file")
        dotenv.load_dotenv()
    except ImportError:
        pass
    webhook_url = getenv("WEBHOOK_URL")
    if not webhook_url:
        raise Exception("Missing env variable: WEBHOOK_URL")

    previous_year_layer = LAYER_NAME_TEMPLATE.format(year=previous_year)
    main(date_var=date_used, layer=previous_year_layer, webhook_url=webhook_url)
    current_year_layer = LAYER_NAME_TEMPLATE.format(year=current_year)
    main(date_var=date_used, layer=current_year_layer, webhook_url=webhook_url)
