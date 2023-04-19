"""
Analysis Example
Configuration parameters for dynamic last value

Set the configurations parameters with the last value of a given variable,
in this example it is the "temperature" variable

Environment Variables
In order to use this analysis, you must setup the Environment Variable table.

account_token: Your account token. Check bellow how to get this.

Steps to generate an account_token:
1 - Enter the following link: https://admin.tago.io/account/
2 - Select your Profile.
3 - Enter Tokens tab.
4 - Generate a new Token with Expires Never.
5 - Press the Copy Button and place at the Environment Variables tab of this analysis.
"""
from queue import Queue
from datetime import datetime

from tagoio_sdk import Account, Analysis
from tagoio_sdk.modules.Utils.envToJson import envToJson
from tagoio_sdk.modules.Utils.getDevice import getDevice


def get_param(params: list, key: str) -> dict:
    """Get the desired parameter from the list of parameters

    Args:
        params (list): list of parameters
        key (str): parameter desired to return

    Returns:
        dict: object with the key and value of the parameter you chose
    """
    return next(
        (x for x in params if x["key"] == key),
        {"key": key, "value": "-", "sent": False},
    )


def apply_device_calculation(device: dict, timezone: str) -> None:
    deviceID, name, account = device["id"], device["name"], device["account"]
    deviceInfoText = f"{name}({deviceID})"
    print(f"Processing Device {deviceInfoText})")
    device = getDevice(account, deviceID)

    # Get the temperature variable inside the device bucket.
    # notice it will get the last record at the time the analysis is running.
    dataResult = device.getData({"variables": ["temperature"], "query": "last_value"})
    if not dataResult:
        print(f"No data found for {deviceInfoText}")
        return

    # Get configuration params list of the device
    deviceParams = account.devices.paramList(deviceID)

    # get the variable temperature from our dataResult array
    temperature = next(
        (data for data in dataResult if data["variable"] == "temperature"), None
    )
    if temperature:
        # get the config. parameter with key temperature
        temperatureParam = get_param(deviceParams, "temperature")
        # get the config. parameter with key last_record_time
        lastRecordParam = get_param(deviceParams, "last_record_time")

        timeString = (
            datetime.fromtimestamp(temperature["time"])
            .astimezone(timezone)
            .strftime("%Y/%m/%d %I:%M %p")
        )

        # creates or edit the tempreature Param with the value of temperature.
        # creates or edit the last_record_time Param with the time of temperature.
        # Make sure to cast the value to STRING, otherwise you'll get an error.
        account.devices.paramSet(
            deviceID,
            [
                {**temperatureParam, "value": str(temperature["value"])},
                {**lastRecordParam, "value": timeString},
            ],
        )


def my_analysis(context: any, scope: list = None) -> None:
    environment = envToJson(context.environment)

    if not environment.get("account_token"):
        raise ValueError("Missing account_token environment var")
    # Make sure you have account_token tag in the environment variable of the analysis.
    account = Account({"token": environment["account_token"]})

    # Create a queue, so we don't run on Throughput errors.
    # The queue will make sure we check only 5 devices simultaneously.
    processQueue = Queue(maxsize=5)
    processQueue.put_nowait(apply_device_calculation)

    # fetch device list filtered by tags.
    # Device list always return an Array with DeviceInfo object.
    deviceList = account.devices.listDevice(
        {
            "amount": 500,
            "fields": ["id", "name", "tags"],
            "filter": {"tags": [{"key": "type", "value": "sensor"}]},
        }
    )

    for device in deviceList:
        processQueue.put(
            device={"id": device["id"], "name": device["name"], "account": account},
            timezone=account.info().get("timezone", "America/New_York"),
        )

    # Wait for all queue to be processed
    processQueue.join()


# The analysis token in only necessary to run the analysis outside TagoIO
Analysis(params={"token": "MY-ANALYSIS-TOKEN-HERE"}).init(my_analysis)
