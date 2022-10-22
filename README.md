[![Issues](https://img.shields.io/github/issues/open-lifeworlds/open-lifeworlds-data)](https://github.com/open-lifeworlds/open-lifeworlds-data/issues)

<br />
<p align="center">
  <a href="https://github.com/open-lifeworlds/open-lifeworlds-data">
    <img src="logo_with_text.png" alt="Logo" height="80">
  </a>

  <h1 align="center">Open Lifeworlds (Data)</h1>

  <p align="center">
    Data for <a href="https://github.com/open-lifeworlds/open-lifeworlds-data" target="_blank">Open
     Lifeworlds data</a> 
  </p>
</p>

## About The Project

The aim of this project is to provide raw data related to LORs.

### Built With

* [Python](https://www.python.org/)

## Installation

Install the following dependencies to fulfill the requirements for this project to run.

```shell script
python -m pip install --upgrade pip
pip install flake8 pytest
pip install requests
pip install pandas
pip install pyproj
```

## Usage

Run this command to start the main script.

```shell script
python main.py [OPTION]...

  -h, --help                           show this help
  -c, --clean                          clean intermediate results before start
  -q, --quiet                          do not log outputs

Examples:
  python main.py -c -p
```

## Roadmap

See the [open issues](https://github.com/open-lifeworlds/open-lifeworlds-data/issues) for a list of proposed features (and
 known issues).

## License

Distributed under the GPLv3 License. See [LICENSE.md](./LICENSE.md) for more information.

## Contact

openlifeworlds@gmail.com
