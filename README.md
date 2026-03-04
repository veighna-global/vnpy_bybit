# Bybit trading gateway for VeighNa Evo

<p align="center">
    <img src ="https://github.com/veighna-global/vnpy_evo/blob/dev/logo.png" width="300" height="300"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-2026.03.04-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.10|3.11|3.12|3.13-blue.svg" />
    <img src ="https://img.shields.io/github/license/veighna-global/vnpy_bybit.svg?color=orange"/>
</p>

## Introduction

This gateway is developed based on Bybit's V5 REST and Websocket API, and supports spot, linear contract, inverse contract and option trading.

**For derivatives contract trading, please notice:**

1. Only supports unified trading account.
2. Only supports cross margin and portfolio margin.
3. Only supports one-way position mode.

## Install

Users can easily install ``vnpy_bybit`` by pip according to the following command.

```
pip install vnpy_bybit
```

Also, users can install ``vnpy_bybit`` using the source code. Clone the repository and install as follows:

```
git clone https://github.com/veighna-global/vnpy_bybit.git && cd vnpy_bybit

pip install -e .
```

## A Simple Example

Save this as run.py.

```
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy_bybit import BybitGateway


def main() -> None:
    """main entry"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(BybitGateway)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()

```
