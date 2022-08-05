#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Author: alfredo
# @Date:   25-07-2022 11:36:33
# @Last Modified by:   alfredo
# @Last Modified time: 2022-07-25 19:10:08

import os
import sys
import paho.mqtt.client as mqtt
import configparser
import argparse
import time
from datetime import datetime
import json

# CHECK LOCK
import signal, errno
from contextlib import contextmanager
import fcntl

import logging
from logging import handlers, Formatter
import traceback

import get_config
import kafka_producer

def check_lock(to=15):
  # ===========================================================================
  # SISTEMA DI LOCK PYTHON2/3
  global file_handler

  class TimeoutException(Exception): pass
  @contextmanager
  def time_limit(seconds):
      def signal_handler(signum, frame):
          raise TimeoutException("Timed out!")

      signal.signal(signal.SIGALRM, signal_handler)
      signal.alarm(seconds)
      try:
          yield
      finally:
          signal.alarm(0)
  try:
      with time_limit(to):
        logger.debug('Inizio Acquisizione Lock...')
        file_handler = open(lock_file, "w")
        fcntl.flock(file_handler.fileno(), fcntl.LOCK_EX)
        logger.debug('Lock acquisito correttamente...')
  except TimeoutException as e:
      logger.info("Lock Timeout: un altro processo di acquisizione dati è in corso... ESCO")
      quit()
  # ===========================================================================


def setup_logger(logger_name, log_file, type='default', level=logging.INFO):
  if(type=='influx'):
    ###
    # LOGGER INFLUX
    ###############
    logger = logging.getLogger(logger_name)
    handler = handlers.RotatingFileHandler(log_file,
                                           maxBytes=1000000,
                                           backupCount=50)
    log_format = Formatter("%(message)s")
    handler.setFormatter(log_format)
    logger.addHandler(handler)
    logger.setLevel(level)
    # Decommentare per mandare l'output del logging a video
    to_screen = logging.StreamHandler(sys.stdout)
    logger.addHandler(to_screen)
  else:
    ###
    # LOGGER NORMALE
    ################
    logger = logging.getLogger(logger_name)
    handler = handlers.RotatingFileHandler(log_file,
                                           maxBytes=500000,
                                           backupCount=15)
    log_format = Formatter("%(asctime)s %(levelname)s : %(message)s", "%d/%m/%Y %H:%M:%S")
    handler.setFormatter(log_format)
    logger.addHandler(handler)
    logger.setLevel(level)
    # Decommentare per mandare l'output del logging a video
    to_screen = logging.StreamHandler(sys.stdout)
    logger.addHandler(to_screen)


def on_connect(client, userdata, flags, rc):
    client.subscribe(cfg['mqtt_topic'], qos=cfg['mqtt_qos'])
    logger.info("CONNESSO su topic: {}".format(cfg['mqtt_topic']))


def on_message(client, userdata, msg):
  global ct
  # logger.info(msg.topic+" "+str(msg.payload))
  ct += 1
  logger.debug("{:>7}) Topic:{}  payload:{} qos:{} retain:{}".format(ct, msg.topic, msg.payload, msg.qos, msg.retain))

  if (args.test):
    # Modalità test... non proseguo
    return



  try:
    #converto da bynary_string > string > dict
    # dict_payload=eval(msg.payload.decode('utf-8'))
    data=(msg.payload.decode('utf-8'))
    header={}
    header=[('tag1', b'tag1'), ('tag2', b'tag2'), ('tag3', b'tag3')]

    kafka_producer.main(cfg['kafka_topic'], data, header)
  except Exception as e:
    logger.error(str(e))



def on_publish(client, userdata, mid):
  logger.info("Message Published... {}".format(str(mid)))


#  ************************************************************************************************************

dirname, filename = os.path.split(os.path.abspath(__file__))
hostname = os.uname()[1]
miopid   = os.getpid()
ct       = 0

###
# Gestione argomenti linea di comando
######################################
msg_help  = '''Es.
     ./mqtt.py
'''


parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                 description='''MQTT''',
                                 epilog=msg_help)

parser.add_argument('-D','--debug',help='Modalita DEBUG',action='store_true' )
parser.add_argument('-t','--test',help='Modalita TEST',action='store_true' )

args = parser.parse_args()


# -------------------------------------------------------------------------------------------
# Sezione Log FILE
# -------------------------------------------------------------------------------------------
if (args.debug):
  livello_log = logging.DEBUG
else:
  livello_log = logging.INFO


lock_file     = '/tmp/mqtt_subscriber.lck'
setup_logger('mqtt_log', dirname+"/log/mqtt_subscriber.log", 'default', livello_log)
logger        = logging.getLogger('mqtt_log')



if not( os.path.isfile(dirname+"/config.cfg") ):
  logger.error("File di configurazione mancante")
  quit()




logger.info('******* MQTT SUBSCRIBER *******')

if (args.test):
  logger.info('******* MODALITA TEST *******')


# verifico se esiste altro processo in corso
check_lock()

###
# LEGGO I PARAMETRI DI CONFIGURAZIONE
#####################################
cfg=get_config.get_config(dirname+"/config.cfg", logger)


client = mqtt.Client(client_id=hostname, clean_session=True, userdata=None, transport=cfg['mqtt_transport'])

# client.reinitialise()
# client.reinitialise(client_id=hostname, clean_session=True, userdata=None)

client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish


if(cfg['mqtt_mode']=='TLS'):
  client.tls_set()

client.username_pw_set(username=cfg['mqtt_user'], password=cfg['mqtt_password'])

def cycle_node_mqtt_server(list_node):
  for srv_mqtt in (list_node):
    logger.debug("Provo a connettermi al nodo: {} ...".format(srv_mqtt))

    try:
      client.connect(srv_mqtt, port=cfg['mqtt_port'], keepalive=cfg['mqtt_keepalive'])
      return True
    except Exception as e:
      logger.debug("Connessione {} non riuscita {}".format(srv_mqtt, str(e)))
      continue

  return False



if(cycle_node_mqtt_server(cfg['mqtt_url'])):
  client.loop_forever()
else:
  logger.error("Impossibile connettersi ad uno dei server MQTT configurati")
  quit()
