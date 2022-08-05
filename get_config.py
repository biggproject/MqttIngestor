#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Author: alfredo
# @Date:   25-07-2022 11:36:33
# @Last Modified by:   alfredo
# @Last Modified time: 2022-07-26 10:46:13

import configparser
from pprint import pformat
import json


def get_config(cfg_file, logger):
  config = configparser.ConfigParser()
  config.read(cfg_file)
  
  cfg={}
  # cfg['mqtt_url']            = config.get('MQTT', 'mqtt_url')
  cfg['mqtt_url']            = json.loads(config.get('MQTT', 'mqtt_url'))
  cfg['mqtt_mode']           = config.get('MQTT', 'mqtt_mode')
  cfg['mqtt_topic']           = config.get('MQTT', 'mqtt_topic')
  cfg['mqtt_port']           = int(config.get('MQTT', 'mqtt_port'))
  cfg['mqtt_keepalive']      = int(config.get('MQTT', 'mqtt_keepalive'))
  cfg['mqtt_user']           = config.get('MQTT', 'mqtt_user')
  cfg['mqtt_password']       = config.get('MQTT', 'mqtt_password')
  cfg['mqtt_transport']      = config.get('MQTT', 'mqtt_transport')
  cfg['mqtt_qos']            = int(config.get('MQTT', 'mqtt_qos'))

  cfg['kafka_topic']               = config.get('KAFKA', 'topic')
  cfg['kafka_bootstrap_servers']   = json.loads(config.get('KAFKA', 'bootstrap_servers'))
  cfg['kafka_auto_offset_reset']   = config.get('KAFKA', 'auto_offset_reset')
  


  
  # if(logger):
  #   logger.debug("--- GET_CONFIG ---")
  #   logger.debug(pformat(cfg))
  # else:  
  #   print("--- GET_CONFIG ---")
  #   print(pformat(cfg))

  return cfg

if __name__ == '__main__':
   get_config("config.cfg", False)