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
import threading
from queue import Queue

from tagoio_sdk import Resources, Analysis


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
    deviceID, name = device["id"], device["name"]
    print(f"Processing Device {name} - ID {deviceID}")
    resources = Resources()

    # Get the temperature variable inside the device bucket.
    # notice it will get the last record at the time the analysis is running.
    dataResult = resources.devices.getDeviceData(deviceID, {"variables": ["temperature"], "query": "last_value"})
    if not dataResult:
        print(f"No data found for {name} - ID {deviceID}")
        return

    # Get configuration params list of the device
    deviceParams = resources.devices.paramList(deviceID)

    # get the variable temperature from our dataResult array
    temperature = next(
        (data for data in dataResult if data["variable"] == "temperature"), None
    )
    if temperature:
        # get the config. parameter with key temperature
        temperatureParam = get_param(deviceParams, "temperature")
        # get the config. parameter with key last_record_time
        lastRecordParam = get_param(deviceParams, "last_record_time")

        timeString = temperature["time"].strftime("%Y/%m/%d %I:%M %p")

        # creates or edit the temperature Param with the value of temperature.
        # creates or edit the last_record_time Param with the time of temperature.
        # Make sure to cast the value to STRING, otherwise you'll get an error.
        resources.devices.paramSet(
            deviceID,
            [
                {**temperatureParam, "value": str(temperature["value"])},
                {**lastRecordParam, "value": timeString},
            ],
        )


def my_analysis(context: any, scope: list = None) -> None:
    resources = Resources()

    # The queue will be filled with device information.
    # The parameter maxsize is the maximum size of the queue.
    processQueue = Queue(maxsize=9999)

    # fetch device list filtered by tags.
    # Device list always return an Array with DeviceInfo object.
    deviceList = resources.devices.listDevice(
        {
            "amount": 500,
            "fields": ["id", "name", "tags"],
            "filter": {"tags": [{"key": "type", "value": "sensor"}]},
        }
    )

    timezone = Resources({"token": "MY-PROFILE-TOKEN-HERE"}).account.info().get("timezone", "America/New_York")

    for device in deviceList:
        processQueue.put(item=({"id": device["id"], "name": device["name"]}, timezone))

    def worker():
        while not processQueue.empty():
            device, timezone = processQueue.get()
            apply_device_calculation(device, timezone)
            processQueue.task_done()

    # Start 5 worker threads
    for _ in range(5):
        t = threading.Thread(target=worker)
        t.start()

    # Wait for all queue to be processed
    processQueue.join()


# The analysis token in only necessary to run the analysis outside TagoIO
Analysis.use(my_analysis, params={"token": "MY-ANALYSIS-TOKEN-HERE"})
