# osm_split

Extract Features from OSM Files.

Please read https://tech.marksblogg.com/extracting-osm-features.html for installation and usage instructions.

## Usage Example

Extract parts of central Tokyo.

```bash
$ wget https://download.geofabrik.de/asia/japan/kanto-latest.osm.pbf

$ python main.py \
    --only-h3=842f5abffffffff,842f5a3ffffffff,842f5bdffffffff \
    kanto-latest.osm.pbf
```