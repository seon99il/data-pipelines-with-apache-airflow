import json
import pathlib

import requests

pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

with open("/tmp/rocket_launches.json", "r") as file:
  launches = json.load(file)
  image_urls = [launch['image'] for launch in launches['results']]
  for image_url in image_urls:
    try:
      response = requests.get(image_url)
      image_filename = image_url.split("/")[-1]
      target_file = f"/tmp/images/{image_filename}"
      with open(target_file, "wb") as f:
        f.write(response.content)
      print(f"Downloaded {image_url} to {target_file}")
    except requests.exceptions.MissingSchema as e:
      print(f"{image_url} appears to be an invalid URL.")

    except requests.exceptions.RequestException as e:
      print(f"Failed to download {image_url}")
