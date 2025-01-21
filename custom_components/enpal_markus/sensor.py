"""Platform for sensor integration."""
from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta, datetime
from homeassistant.components.sensor import (SensorEntity)
from homeassistant.core import HomeAssistant
from homeassistant import config_entries
from homeassistant.helpers.device_registry import DeviceEntryType
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_registry import async_get, async_entries_for_config_entry
from custom_components.enpal_markus.const import DOMAIN
import aiohttp
import logging
from influxdb_client import InfluxDBClient

_LOGGER = logging.getLogger(__name__)
SCAN_INTERVAL = timedelta(seconds=20)

VERSION= '0.1.0'

def get_tables(ip: str, port: int, token: str):
    client = InfluxDBClient(url=f'http://{ip}:{port}', token=token, org='enpal')
    query_api = client.query_api()

    query = 'from(bucket: "solar") \
      |> range(start: -1h) \
      |> last()'

    tables = query_api.query(query)
    return tables


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: config_entries.ConfigEntry,
    async_add_entities,
):
    # Get the config entry for the integration
    config = hass.data[DOMAIN][config_entry.entry_id]
    if config_entry.options:
        config.update(config_entry.options)
    to_add = []
    if not 'enpal_host_ip' in config:
        _LOGGER.error("No enpal_host_ip in config entry")
        return
    if not 'enpal_host_port' in config:
        _LOGGER.error("No enpal_host_port in config entry")
        return
    if not 'enpal_token' in config:
        _LOGGER.error("No enpal_token in config entry")
        return
    
    global_config = hass.data[DOMAIN]
        
    tables = await hass.async_add_executor_job(get_tables, config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'])


    for table in tables:
        field = table.records[0].values['_field']
        measurement = table.records[0].values['_measurement']
        unit = table.records[0].values['unit']

        if measurement == "inverter":
            if field == "Current.Battery": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Current.Battery', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'current', unit))
            elif field == "Current.String.1": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Current.String.1', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'current', unit))
            elif field == "Current.String.2": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Current.String.2', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'current', unit))
            elif field == "Temperature.Battery": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:home-thermometer-outline', 'Temperature.Battery', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'temperature', unit))
            elif field == "Frequency.Grid": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:solar-power-variant', 'Frequency.Grid', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'frequency', unit))
            elif field == "Inverter.System.State": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'Inverter.System.State', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'none', unit))
            elif field == "State.ErrorCodes.1": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'State.ErrorCodes.1', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'none', unit))
            elif field == "State.ErrorCodes.10": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'State.ErrorCodes.10', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'none', unit))
            elif field == "State.ErrorCodes.11": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'State.ErrorCodes.11', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'none', unit))
            elif field == "State.ErrorCodes.2": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'State.ErrorCodes.2', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'none', unit))
            elif field == "State.ErrorCodes.6": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'State.ErrorCodes.6', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'none', unit))
            elif field == "State.ErrorCodes.9": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'State.ErrorCodes.9', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'none', unit))
            elif field == "Battery.ChargeLevel.Max": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'Battery.ChargeLevel.Max', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'battery', unit))
            elif field == "Battery.ChargeLevel.Min": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'Battery.ChargeLevel.Min', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'battery', unit))
            elif field == "Battery.ChargeLevel.MinOnGrid": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'attery.ChargeLevel.MinOnGrid', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'battery', unit))
            elif field == "Battery.SOH": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'Battery.SOH', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'battery', unit))
            elif field == "Energy.Battery.Charge.Level": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'Energy.Battery.Charge.Level', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'battery', unit))
            elif field == "Energy.Battery.Charge.Level.Absolute": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'Energy.Battery.Charge.Level.Absolute', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'battery', unit))
            elif field == "Voltage.Battery": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'Voltage.Battery', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'voltage', unit))
            elif field == "Voltage.Phase.A": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Voltage.Phase.A', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'voltage', unit))
            elif field == "Voltage.Phase.B": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Voltage.Phase.B', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'voltage', unit))
            elif field == "Voltage.Phase.C": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Voltage.Phase.C', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'voltage', unit))
            elif field == "Voltage.String.1": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Voltage.String.1', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'voltage', unit))
            elif field == "Voltage.String.2": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Voltage.String.2', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'voltage', unit))
            elif field == "Power.AC.Phase.A": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Power.AC.Phase.A', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            elif field == "Power.AC.Phase.B": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Power.AC.Phase.B', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            elif field == "Power.AC.Phase.C": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Power.AC.Phase.C', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            elif field == "Power.Battery.Charge.Discharge": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'Power.Battery.Charge.Discharge', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            elif field == "Power.DC.String.1": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Power.DC.String.1', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            elif field == "Power.DC.String.2": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Power.DC.String.2', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            elif field == "Power.DC.Total": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:lightning-bolt', 'Power.DC.Total', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            elif field == "Power.Grid.Export": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:home-lightning-bolt', 'Power.Grid.Export', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            elif field == "Power.House.Total": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:home-lightning-bolt', 'Power.House.Total', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            else:
                _LOGGER.debug(f"Not adding measurement: {measurement} field: {field}")

        elif measurement == "iot":
            if field == "Cpu.Load": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'Cpu.Load', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'percent', unit))
            elif field == "Memory.Usage": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'Memory.Usage', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'percent', unit))
            else:
                _LOGGER.debug(f"Not adding measurement: {measurement} field: {field}")

        elif measurement == "system":
            if field == "Percent.Storage.Level": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:battery', 'Percent.Storage.Level', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'percent', unit))
            elif field == "Power.Consumption.Total": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:home-lightning-bolt', 'Power.Consumption.Total', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            elif field == "Power.External.Total": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:home-lightning-bolt', 'Power.External.Total', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            elif field == "Power.Production.Total": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:solar-power', 'Power.Production.Total', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            elif field == "Power.Storage.Total": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:battery-charging', 'Power.Storage.Total', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'power', unit))
            elif field == "Energy.Storage.Level":
                to_add.append(EnpalSensor(field, measurement, 'mdi:test-tube-empty', 'Energy.Storage.Level', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'energy', unit))
            elif field == "Energy.Consumption.Total.Day": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:home-lightning-bolt', 'Energy.Consumption.Total.Day', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'energy', unit))
            elif field == "Energy.External.Total.In.Day": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:transmission-tower-import', 'Energy.External.Total.In.Day', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'energy', unit))
            elif field == "Energy.External.Total.Out.Day": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:transmission-tower-export', 'Energy.External.Total.Out.Day', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'energy', unit))
            elif field == "Energy.Production.Total.Day": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:solar-power-variant', 'Energy.Production.Total.Day', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'energy', unit))
            elif field == "Energy.Storage.Total.In.Day": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:battery-arrow-up', 'Energy.Storage.Total.In.Day', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'energy', unit))
            elif field == "Energy.Storage.Total.Out.Day": 
                to_add.append(EnpalSensor(field, measurement, 'mdi:battery-arrow-down', 'Energy.Storage.Total.Out.Day', config['enpal_host_ip'], config['enpal_host_port'], config['enpal_token'], 'energy', unit))
            else:
                _LOGGER.debug(f"Not adding measurement: {measurement} field: {field}")
        else:
            _LOGGER.debug(f"Measurement type not recognized: {measurement}")
          
    entity_registry = async_get(hass)
    entries = async_entries_for_config_entry(
        entity_registry, config_entry.entry_id
    )
    for entry in entries:
        entity_registry.async_remove(entry.entity_id)

    async_add_entities(to_add, update_before_add=True)


class EnpalSensor(SensorEntity):

    def __init__(self, field: str, measurement: str, icon:str, name: str, ip: str, port: int, token: str, device_class: str, unit: str):
        self.field = field
        self.measurement = measurement
        self.ip = ip
        self.port = port
        self.token = token
        self.enpal_device_class = device_class
        self.unit = unit
        self._attr_icon = icon
        self._attr_name = name
        self._attr_unique_id = f'enpal_markus_{measurement}_{field}'
        self._attr_extra_state_attributes = {}


    async def async_update(self) -> None:

        # Get the IP address from the API
        try:
            client = InfluxDBClient(url=f'http://{self.ip}:{self.port}', token=self.token, org="enpal")
            query_api = client.query_api()

            query = f'from(bucket: "solar") \
              |> range(start: -5m) \
              |> filter(fn: (r) => r["_measurement"] == "{self.measurement}") \
              |> filter(fn: (r) => r["_field"] == "{self.field}") \
              |> last()'

            tables = await self.hass.async_add_executor_job(query_api.query, query)

            value = 0
            if tables:
                value = tables[0].records[0].values['_value']

            if self.field == 'Frequency.Grid':
                if value < 0 or value > 100:
                    return

            if self.field == 'Temperature.Battery':
                if value < -100 or value > 100:
                    return

            self._attr_native_value = round(float(value), 2)
            self._attr_device_class = self.enpal_device_class
            self._attr_native_unit_of_measurement	= self.unit
            self._attr_state_class = 'measurement'
            self._attr_extra_state_attributes['last_check'] = datetime.now()
            self._attr_extra_state_attributes['field'] = self.field
            self._attr_extra_state_attributes['measurement'] = self.measurement

            if self._attr_native_unit_of_measurement == "kWh":
                self._attr_extra_state_attributes['last_reset'] = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                self._attr_state_class = 'total_increasing'
            if self._attr_native_unit_of_measurement == "Wh":
                self._attr_extra_state_attributes['last_reset'] = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                self._attr_state_class = 'total_increasing'

            if self.field == 'Percent.Storage.Level':
                if self._attr_native_value >= 10:
                    self._attr_icon = "mdi:battery-outline"
                if self._attr_native_value <= 19 and self._attr_native_value >= 10:
                    self._attr_icon = "mdi:battery-10"
                if self._attr_native_value <= 29 and self._attr_native_value >= 20:
                    self._attr_icon = "mdi:battery-20"
                if self._attr_native_value <= 39 and self._attr_native_value >= 30:
                    self._attr_icon = "mdi:battery-30"
                if self._attr_native_value <= 49 and self._attr_native_value >= 40:
                    self._attr_icon = "mdi:battery-40"
                if self._attr_native_value <= 59 and self._attr_native_value >= 50:
                    self._attr_icon = "mdi:battery-50"
                if self._attr_native_value <= 69 and self._attr_native_value >= 60:
                    self._attr_icon = "mdi:battery-60"
                if self._attr_native_value <= 79 and self._attr_native_value >= 70:
                    self._attr_icon = "mdi:battery-70"
                if self._attr_native_value <= 89 and self._attr_native_value >= 80:
                    self._attr_icon = "mdi:battery-80"
                if self._attr_native_value <= 99 and self._attr_native_value >= 90:
                    self._attr_icon = "mdi:battery-90"
                if self._attr_native_value == 100:
                    self._attr_icon = "mdi:battery"
                
        except Exception as e:
            _LOGGER.error(f'{e}')
            self._state = 'Error'
            self._attr_native_value = None
            self._attr_extra_state_attributes['last_check'] = datetime.now()
