# HS Deribit Crawler
> HS BitCoin options crawler from Deribit with CSV output

[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)
<a href="https://996.icu"><img src="https://img.shields.io/badge/link-996.icu-red.svg" alt="996.icu"></a>
#
![demo](https://user-images.githubusercontent.com/19645990/30264586-26db6dfa-96a7-11e7-9fa3-10202f26a90f.png)

## Requirement
* Python 2.7 and 3.4+

## Installation
  Get *deribit-api-clients*
  ```
  git clone https://github.com/deribit/deribit-api-clients
  ```
  Copy the python folder into the hs-loader project and then install the client
  ```
  python setup.py install --user
  ```
  Get key and secret from your deribit account. And change the key and secret value in the configuration file
  ```
  python pull_data.py
  ```
