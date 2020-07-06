import time
import os
from threading import Thread
from bluepy.btle import BTLEException
from bluepy.sensortag import SensorTag

IR_TEMP = "ir_temp"
ACCELEROMETER = "accelerometer"
HUMIDITY = "humidity"
MAGNETOMETER = "magnetometer"
BAROMETER = "barometer"
GYROSCOPE = "gyroscope"
BATTERY = "battery"
LIGHT = "light"

DEFINED_SENSORS = [IR_TEMP, ACCELEROMETER, HUMIDITY, MAGNETOMETER, BAROMETER, GYROSCOPE, BATTERY, LIGHT]
INTERESTED_SENSORS = [LIGHT]
OUT_FILE = "lux.csv"
TIME_BETWEEN_READS = 5
TIME_BETWEEN_WRITES = 1
INIT_WAIT = 5
TIME_BETWEEN_RETRY = 5

SENSOR_TAG_LIST = [
    {
        "ble_mac": "54:6C:0E:53:45:B7",
        "label": "a"
    },
    {
        "ble_mac": "54:6C:0E:53:3B:0A",
        "label": "b"
    },
    {
        "ble_mac": "54:6C:0E:53:46:44",
        "label": "c"
    },
    {
        "ble_mac": "54:6C:0E:53:3F:77",
        "label": "d"
    },
    {
        "ble_mac": "54:6C:0E:78:BE:82",
        "label": "e"
    },
    {
        "ble_mac": "F0:F8:F2:86:31:86",
        "label": "f"
    },
]

LUX_READINGS = []


def enable_sensors(tag, sensor_list):
    if IR_TEMP in sensor_list:
        tag.IRtemperature.enable()
    if ACCELEROMETER in sensor_list:
        tag.accelerometer.enable()
    if HUMIDITY in sensor_list:
        tag.humidity.enable()
    if MAGNETOMETER in sensor_list:
        tag.magnetometer.enable()
    if BAROMETER in sensor_list:
        tag.barometer.enable()
    if GYROSCOPE in sensor_list:
        tag.gyroscope.enable()
    if LIGHT in sensor_list:
        tag.lightmeter.enable()
    if BATTERY in sensor_list:
        tag.battery.enable()

    # Some sensors (e.g., temperature, accelerometer) need some time for initialization.
    # Not waiting here after enabling a sensor, the first read value might be empty or incorrect.
    time.sleep(1.0)


def disable_sensors(tag, sensor_list):
    """Disable sensors to improve battery life."""
    if IR_TEMP in sensor_list:
        tag.IRtemperature.disable()
    if ACCELEROMETER in sensor_list:
        tag.accelerometer.disable()
    if HUMIDITY in sensor_list:
        tag.humidity.disable()
    if MAGNETOMETER in sensor_list:
        tag.magnetometer.disable()
    if BAROMETER in sensor_list:
        tag.barometer.disable()
    if GYROSCOPE in sensor_list:
        tag.gyroscope.disable()
    if LIGHT in sensor_list:
        tag.lightmeter.disable()
    if BATTERY in sensor_list:
        tag.battery.disable()


def get_readings(tag, sensor_list):
    """Get sensor readings and collate them in a dictionary."""
    try:
        enable_sensors(tag, sensor_list)
        readings = {}
        timestamp = int(time.time())
        if IR_TEMP in sensor_list:
            readings["ir_temp"], readings["ir"] = tag.IRtemperature.read()
        if ACCELEROMETER in sensor_list:
            readings["x_accel"], readings["y_accel"], readings["z_accel"] = tag.accelerometer.read()
        if HUMIDITY in sensor_list:
            readings["humidity_temp"], readings["humidity"] = tag.humidity.read()
        if MAGNETOMETER in sensor_list:
            readings["x_magnet"], readings["y_magnet"], readings["z_magnet"] = tag.magnetometer.read()
        if BAROMETER in sensor_list:
            readings["baro_temp"], readings["pressure"] = tag.barometer.read()
        if GYROSCOPE in sensor_list:
            readings["x_gyro"], readings["y_gyro"], readings["z_gyro"] = tag.gyroscope.read()
        if LIGHT in sensor_list:
            readings["light"] = tag.lightmeter.read()
        if BATTERY in sensor_list:
            readings["battery"] = tag.battery.read()

        disable_sensors(tag, sensor_list)

        # round to 2 decimal places for all readings
        readings = {key: round(value, 2) for key, value in readings.items()}
        readings["timestamp"] = timestamp
        return readings

    except BTLEException as e:
        print("Unable to take sensor readings.")
        print(e)
        return {}


def get_new_tag_reference(ble_mac, label):
    print(ble_mac, label, "re-connecting...")
    tag = None
    while not tag:
        try:
            tag = SensorTag(ble_mac)
        except Exception as e:
            print(ble_mac, label, str(e))
            print("will retry in %d seconds"%TIME_BETWEEN_RETRY)
            time.sleep(TIME_BETWEEN_RETRY)
    print(ble_mac, label, "re-connected")
    return tag


def collect_lux_readings(label, ble_mac):
    print(ble_mac, label, "starting collection thread")
    print(ble_mac, label, "connecting...")
    tag = None
    while not tag:
        try:
            tag = SensorTag(ble_mac)
        except Exception as e:
            print(ble_mac, label, str(e))
            print("will retry in %d seconds" % TIME_BETWEEN_RETRY)
            time.sleep(TIME_BETWEEN_RETRY)
    print(ble_mac, label, "connected")
    while 1:
        timestamp = TIME_BETWEEN_READS+1
        while timestamp % TIME_BETWEEN_READS != 0:  # for the sync purposes between other recordings
            time.sleep(0.5)
            timestamp = int(time.time())
            timestamp = timestamp - 1  # to compensate the saturation time after turning the sensors on
        readings = get_readings(tag, INTERESTED_SENSORS)
        if not readings:
            tag = get_new_tag_reference(ble_mac, label)
            continue
        readings["label"] = label
        LUX_READINGS.append(readings)


def process_readings():
    print("starting processing thread")
    while 1:
        current_records_number = len(LUX_READINGS)
        if current_records_number > 0:
            if not os.path.isfile(OUT_FILE):
                create_csv_file_with_header(OUT_FILE, sorted(LUX_READINGS[0]))
            i = 0
            with open(OUT_FILE, 'a') as f:
                while i < current_records_number:
                    values = []
                    readings = LUX_READINGS.pop()
                    with open(OUT_FILE, "a") as f:
                        for k in sorted(readings):
                            values.append(readings[k])
                        f.write(",".join([str(x) for x in values]) + "\n")
                    i += 1
        time.sleep(TIME_BETWEEN_WRITES)


def create_csv_file_with_header(file_name, header):
    header_line = ','.join(header)
    print("creating file with header,", header)
    with open(file_name, 'w') as f:
        f.write(header_line + '\n')


def main():
    start_time = int(time.time())
    print('init time', start_time)
    for sensor_tag in SENSOR_TAG_LIST:
        Thread(target=collect_lux_readings, args=(sensor_tag["label"], sensor_tag["ble_mac"])).start()
        time.sleep(1)
    print("going to sleep for seconds", INIT_WAIT)
    time.sleep(INIT_WAIT)
    process_readings()


if __name__ == "__main__":
    main()
