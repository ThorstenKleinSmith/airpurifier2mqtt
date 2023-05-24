from timeit import default_timer as timer
import functools
import argparse
import asyncio
import io
import json
import logging
import yaml

from dotmap import DotMap
from hbmqtt.client import ConnectException
from hbmqtt.client import MQTTClient
from hbmqtt.mqtt.constants import QOS_0
from miio.airpurifier_miot import AirPurifierMiotStatus
from miio.fan_miot import FanStatusMiot
import miio

class ConfigurationException(Exception):
    """Configuration error"""
    pass

class StateEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, AirPurifierMiotStatus):
            return {
                'temperature': o.temperature,
                'power': o.power.capitalize(),
                'aqi': o.aqi,
                'average_aqi': o.average_aqi,
                'humidity': o.humidity,
                'fan_level': o.fan_level,
                'filter_hours_used': o.filter_hours_used,
                'filter_life_remaining': o.filter_life_remaining,
                'favorite_level': o.favorite_level,
                'child_lock': o.child_lock,
                'led': o.led,
                'motor_speed': o.motor_speed,
                'purify_volume': o.purify_volume,
                'use_time': o.use_time,
                'buzzer': o.buzzer,
                'filter_rfid_product_id': o.filter_rfid_product_id,
                'filter_rfid_tag': o.filter_rfid_tag,
                'mode': o.mode.name,
                'led_brightness': o.led_brightness.name,
                'filter_type': o.filter_type.name
            }
        if isinstance(o, FanStatusMiot):
            return {
                'angle': o.angle,
                'buzzer': o.buzzer,
                'child_lock': o.child_lock,
                'delay_off_countdown': o.delay_off_countdown,
                'is_on': o.is_on,
                'led': o.led,
                'mode': o.mode.name,
                'oscillate': o.oscillate,
                'power': o.power.capitalize(),
                'speed': o.speed
            }
        return json.JSONEncoder.default(self, o)

def _device_status(device: miio.miot_device.MiotDevice, log: logging.Logger):
    """
    Gets device status asynchronously.

    Returns device status or None in case status cannot be fetched or some
    errors occured during fetching.
    """
    def status(device: miio.miot_device.MiotDevice, log: logging.Logger):
        log.debug('Polling state...')
        try:
            polling_start = timer()
            status = device.status()
            status_json = json.dumps(status, cls=StateEncoder).encode('utf-8')
            log.debug('Polling state succeeded and took %.3fs', timer() - polling_start)
            log.info('Polled state %s', status_json)
            return status_json
        except Exception as error:
            log.warning('Polling state failed and took %.3fs. Reason is %s: %s',
                timer() - polling_start,
                '.'.join([error.__class__.__module__, error.__class__.__qualname__]),
                error)
            raise
    loop = asyncio.get_running_loop()
    return loop.run_in_executor(None, status, device, log)

def _device_command(log: logging.Logger, device: miio.miot_device.MiotDevice, name: str, *args):
    def command(device: miio.miot_device.MiotDevice, name: str, log: logging.Logger, *args):
        try:
            command_start = timer()
            getattr(device, name)(*args)
            log.debug('Command "%s" succeeded and took %.3fs',
                name,
                timer() - command_start)
            return True
        except Exception as error:
            log.warning('Command "%s" failed and took %.3fs. Reason is %s: %s',
                name,
                timer() - command_start,
                '.'.join([error.__class__.__module__, error.__class__.__qualname__]),
                error)
            raise
    loop = asyncio.get_running_loop()
    return loop.run_in_executor(None, command, device, name, log, *args)

def _retry(coro, retries: int = 3, interval: float = 1, fail_result = None, log = None):
    """
    Retries coroutine
    """
    async def _retry_wrapper(*args, **kwargs):
        nonlocal retries
        retries = 0 if retries < 0 else retries
        while retries >= 0:
            try:
                return await coro(*args, **kwargs)
            except Exception:
                if retries <= 0:
                    return fail_result
                if log:
                    log.info('Retry in %.3fsec. Retries left: %d',
                        interval,
                        retries)
                retries -= 1
                await asyncio.sleep(interval)
        return fail_result
    return _retry_wrapper

async def _create_mqtt_client(mqtt_config: DotMap, log: logging.Logger = None):
    """
    Create new MQTT client and connect to MQTT broker
    """
    log = log if not log is None else logging.getLogger('miio2mqtt.mqtt')

    mqtt_client = MQTTClient(config=mqtt_config.client)
    # todo: add MQTTClient.handle_connection_close()
    # called from task.set_exception(ClientException("Connection lost"))
    try:
        log.info('Connecting to MQTT')
        await mqtt_client.connect(
            uri=mqtt_config.client.uri,
            cleansession=mqtt_config.client.cleansession)
        return mqtt_client
    except ConnectException as connection_exception:
        log.error('Can\'t connect to MQTT: %s', connection_exception)
        raise
    finally:
        log.info('Connected to MQTT')

async def mqtt_publisher(mqtt: DotMap, device_status_queue: asyncio.Queue):
    log = logging.getLogger('miio2mqtt.mqtt.publisher')

    mqtt_client = await _create_mqtt_client(mqtt, log=log)
    while True:
        device_name, device_status, is_state = await device_status_queue.get()
        if is_state:
            mqtt_topic = '{}/{}/state'.format(mqtt.topic_prefix, device_name)
        else:
            device_status = device_status.encode('utf-8')
            mqtt_topic = '{}/{}/status'.format(mqtt.topic_prefix, device_name)
        log.debug('Publishing: topic=%s payload=%s', mqtt_topic, device_status)
        await mqtt_client.publish(mqtt_topic, device_status)

async def mqtt_subscriber(mqtt: DotMap, device_command_queues: dict):
    log = logging.getLogger('miio2mqtt.mqtt.subscriber')

    mqtt_client = await _create_mqtt_client(mqtt, log=log)
    await mqtt_client.subscribe([
        ('{}/+/set'.format(mqtt.topic_prefix), QOS_0),
        ('{}/+/set/+'.format(mqtt.topic_prefix), QOS_0)])
    while True:
        message = await mqtt_client.deliver_message()
        log.debug('Got message topic=%s payload=%s', message.topic, message.data.decode('utf-8'))
        topic_parts = message.topic.split('/')[1:]
        device_name = topic_parts.pop(0)
        topic_parts.pop(0) # pop 'set'
        device_property = topic_parts.pop(0) if len(topic_parts) == 1 else None
        if not device_name in device_command_queues:
            log.error('Unknown device "%s"', device_name)
            continue
        payload = message.data.decode('utf-8')
        try:
            if device_property is None:
                payload_json = json.loads(payload)
            else:
                payload_json = { device_property: payload }
            device_command_queues[device_name].put_nowait(payload_json)
            log.debug('Scheduled command for device "%s"', device_name)
        except asyncio.QueueFull:
            log.warning('Scheduling command for device "%s" failed. Device command queue is full!',
                device_name)
        except json.decoder.JSONDecodeError as json_error:
            log.error('Error parsing JSON payload "%s". %s: %s',
                payload,
                '.'.join([json_error.__class__.__module__, json_error.__class__.__qualname__]),
                json_error)

def _create_device(device_config: DotMap):
    if device_config.type == 'AirPurifierMiot':
      return miio.AirPurifierMiot(device_config.ip, device_config.token)
    elif device_config.type == 'FanP10':
      return miio.fan_miot.FanP10(device_config.ip, device_config.token)


async def airpurifier_device_command(device_config: DotMap, device_command_queue: asyncio.Queue, force_poll_event: asyncio.Event):
    """
    Sends commands enqueued in `device_command_queue` to device
    """
    log = logging.getLogger('miio2mqtt.command.{}'.format(device_config.name))
    while True:
        command_json = await device_command_queue.get()
        device = _create_device(device_config)
        command = functools.partial(_retry(_device_command, fail_result=False, log=log), log, device)
        log.debug('Processing command for device "%s": %s', device_config.name, command_json)
        for property_name, property_value in command_json.items():
            try:
                if property_name == 'power':
                    val = property_value.lower()
                    if not val in ('on', 'off'):
                        raise ValueError('Value must be "on" or "off" but was {}'.format(val))
                    await command(val)
                elif property_name == 'mode':
                    val = property_value.capitalize()
                    if not val in ('Auto', 'Silent', 'Favorite', 'Fan'):
                        raise ValueError('Value must be "Auto", "Silent", "Favorite" or "Fan"  but was {}'.format(val))
                    await command('set_mode', miio.airpurifier_miot.OperationMode[val])
                elif property_name == 'favorite_level':
                    val = int(property_value)
                    if  not 0 <= val <= 14:
                        raise ValueError('Value must be between 0 and 14 but was {}'.format(val))
                    await command('set_favorite_level', val)
                else:
                    log.error('Unknown command "%s" or invalid command value or value type "%s"',
                        property_name,
                        property_value)
            except Exception as error:
                log.error('Cannot process command "%s". Reason: %s', property_name, error)
                continue
        force_poll_event.set()

async def fan_device_command(device_config: DotMap, device_command_queue: asyncio.Queue, force_poll_event: asyncio.Event):
    """
    Sends commands enqueued in `device_command_queue` to device
    """
    log = logging.getLogger('miio2mqtt.command.{}'.format(device_config.name))
    while True:
        command_json = await device_command_queue.get()
        device = _create_device(device_config)
        command = functools.partial(_retry(_device_command, fail_result=False, log=log), log, device)
        log.debug('Processing command for device "%s": %s', device_config.name, command_json)
        for property_name, property_value in command_json.items():
            try:
                if property_name == 'angle':
                    val = property_value
                    if isinstance(val, str):
                        val = int(val)
                    if not isinstance(val, int) or not val in (30, 60, 90, 120, 140):
                        raise ValueError('Value must be 30, 60, 90, 120 or 140 but was {}'.format(val))
                    log.info('setting angle to %i', val)
                    await command('set_angle', val)
                elif property_name == 'buzzer':
                    val = property_value
                    if not isinstance(val, bool):
                        raise ValueError('Value must be true or false but was {}'.format(val))
                    log.info('setting buzzer to %s', val)
                    await command('set_buzzer', val)
                elif property_name == 'child_lock':
                    val = property_value
                    if not isinstance(val, bool):
                        raise ValueError('Value must be true or false but was {}'.format(val))
                    log.info('setting child lock to %s', val)
                    await command('set_child_lock', val)
                elif property_name == 'delay_off_countdown':
                    val = property_value
                    if isinstance(val, str):
                        val = int(val)
                    if not isinstance(val, int) or val < 0 or val > 480:
                        raise ValueError('Value must be a number between 0 and 140 but was {}'.format(val))
                    log.info('setting off delay to %i', val)
                    await command('delay_off', val)
                elif property_name == 'led':
                    val = property_value
                    if not isinstance(val, bool):
                        raise ValueError('Value must be true or false but was {}'.format(val))
                    log.info('setting led to %s', val)
                    await command('set_led', val)
                elif property_name == 'mode':
                    val = property_value.capitalize()
                    if not val in ('Nature', 'Normal'):
                        raise ValueError('Value must be "Nature" or "Normal" but was {}'.format(val))
                    log.info('setting mode to "%s"', val)
                    await command('set_mode', miio.fan_common.OperationMode[val])
                elif property_name == 'oscillate':
                    val = property_value
                    if not isinstance(val, bool):
                        raise ValueError('Value must be true or false but was {}'.format(val))
                    log.info('setting oscillate to %s', val)
                    await command('set_oscillate', val)
                elif property_name == 'power':
                    val = property_value.lower()
                    if not val in ('on', 'off'):
                        raise ValueError('Value must be "on" or "off" but was {}'.format(val))
                    log.info('setting power to "%s"', val)
                    await command(val)
                elif property_name == 'rotate':
                    val = property_value.capitalize()
                    if not val in ('Left', 'Right'):
                        raise ValueError('Value must be "Left" or "Right" but was {}'.format(val))
                    log.info('rotating "%s"', val)
                    await command('set_rotate', miio.fan_common.MoveDirection[val])
                elif property_name == 'speed':
                    val = property_value
                    if isinstance(val, str):
                        val = int(val)
                    if not isinstance(val, int) or val < 1 or val > 100:
                        raise ValueError('Value must be a number between 1 and 100 but was {}'.format(val))
                    log.info('setting speed to %i', val)
                    await command('set_speed', val)
                else:
                    log.error('Unknown command "%s" or invalid command value or value type "%s"',
                        property_name,
                        property_value)
            except Exception as error:
                log.error('Cannot process command "%s". Reason: %s', property_name, error)
                continue
        force_poll_event.set()

async def device_polling(device_config: DotMap, device_status_queue: asyncio.Queue, force_poll_event: asyncio.Event):
    log = logging.getLogger('miio2mqtt.state.{}'.format(device_config.name))
    device = _create_device(device_config)
    is_online = None
    while True:
        if force_poll_event.is_set():
            log.debug('Polling device state has been forced')
            # sleep some time before polling state to get a device
            # time to update it's state
            await asyncio.sleep(1)
        retryable_device_status = _retry(
            _device_status,
            retries = device_config.polling.get('retries', 0),
            interval = device_config.polling.get('retry_interval', 10),
            log = log)
        status = await retryable_device_status(device, log)
        if status is not None:
            if device_status_queue.full():
                log.warning('Status queue is full. Polling will be suspended')
            await device_status_queue.put((device_config.name, status, True))
            log.debug('Device status enqueued')
            if is_online == None or not is_online:
                if device_status_queue.full():
                    log.warning('Status queue is full. Polling will be suspended')
                await device_status_queue.put((device_config.name, 'online', False))
                log.info('Device %s online status enqueued', device_config.name)
                is_online = True
        else:
            if is_online == None or is_online:
                if device_status_queue.full():
                    log.warning('Status queue is full. Polling will be suspended')
                await device_status_queue.put((device_config.name, 'offline', False))
                log.info('Device %s offline status enqueued', device_config.name)
                is_online = False
        polling_interval = device_config.polling.get('interval', 120)
        log.debug('Next polling in %ds', polling_interval)
        force_poll_event.clear()
        await asyncio.wait([force_poll_event.wait()], timeout=polling_interval)

async def start(config: DotMap):
    """
    This is coroutine.

    Starts polling device state and publishing it to mqtt.
    Subscribes mqtt for commands and sends them to device.
    """
    log = logging.getLogger('miio2mqtt')
    if config.mqtt.client.will.message:
        log.debug('converting mqtt will message "%s"', config.mqtt.client.will.message)
        config.mqtt.client.will.message = bytes(config.mqtt.client.will.message, 'utf-8')
    log.info('config: %s', config)

    devices_status_queue = asyncio.Queue(3 * len(config.devices))
    await devices_status_queue.put(('Controller', 'online', False))

    tasks = []
    device_command_queues = {}
    for device in config.devices:
        # create polling task
        force_poll_event = asyncio.Event()
        task_name = 'device-polling-{}'.format(device.name)
        task = asyncio.create_task(device_polling(device, devices_status_queue, force_poll_event), name=task_name)
        tasks.append(task)

        # create command task
        task_name = 'device-command-{}'.format(device.name)
        command_queue = asyncio.Queue(64)
        device_command_queues[device.name] = command_queue
        if device.type == 'AirPurifierMiot':
          task = asyncio.create_task(airpurifier_device_command(device, command_queue, force_poll_event), name=task_name)
        elif device.type == 'FanP10':
          task = asyncio.create_task(fan_device_command(device, command_queue, force_poll_event), name=task_name)
        tasks.append(task)

    tasks.append(asyncio.create_task(mqtt_publisher(config.mqtt, devices_status_queue), name='mqtt-publisher'))
    tasks.append(asyncio.create_task(mqtt_subscriber(config.mqtt, device_command_queues), name='mqtt-subscriber'))

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    for task in done:
        try:
            task.result()
        except Exception as error:
            log.exception('Finishing because of error: %s', error)
    for task in pending:
        task.cancel()

def _to_config(dictionary: DotMap):
    if not dictionary.devices:
        raise ConfigurationException('No devices configured')
    device_names = []
    for device in dictionary.devices:
        if not device.name:
            raise ConfigurationException('One of the devices is missing the name')
        if device.name in device_names:
            raise ConfigurationException('Device "{name}" is defined more than once'\
                .format(**device))
        device_names.append(device.name)
        if not device.type:
            raise ConfigurationException('One of a device is missing type')
        if not device.type in ('AirPurifierMiot', 'FanP10'):
            raise ConfigurationException('Device "{name}" has an invalid type'\
                .format(**device))
        if not device.ip:
            raise ConfigurationException('Device "{name}" is missing the ip'\
                .format(**device))
        if not device.token:
            raise ConfigurationException('Device "{name}" is missing the token'\
                .format(**device))
    return dictionary

def _main():
    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()

    # read config file
    with io.open(args.config, 'r') as stream:
        config = _to_config(DotMap(yaml.safe_load(stream)))

    # configure logging
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s%(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    # read loggers configuration from config
    for logger_name, logger_level in config.logging.items():
        logging.getLogger(None if logger_name == 'root' else logger_name).setLevel(logger_level)

    asyncio.run(start(config))

if __name__ == "__main__":
    _main()
