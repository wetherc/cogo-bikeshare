# Prerequisites

This project uses Uber's [https://github.com/uber/h3](H3) system for geospatial indexing. Installation instructions can be found in their repository. Broadly, you'll need `cc`, `make`, and `cmake` available in your path. If you're on Mac, you can just `brew install h3` and you're good to go. Uber's documetion contains additional information for compiling from source if you're on a different platform or just enjoy the thrill of it.

NOTE: `cmake` **must** also be in your path to install the H3 Python bindings.

# Installation and Usage

Just
```
cd /path/to/cogo
pip install -r requirements.txt
jupyter lab
```
and you're off to the races.
