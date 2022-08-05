#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Author: alfredo
# @Date:   25-07-2022 11:36:33
# @Last Modified by:   alfredo
# @Last Modified time: 2022-07-26 10:46:15

import os
import sys
import configparser
import argparse
import logging
from logging import handlers, Formatter
import traceback
import get_config
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import uuid
import pytz
import datetime

def date_iso8601(tz="Europe/Rome"):
  tz = pytz.timezone(tz)
  dt = tz.localize(datetime.datetime.now())
  return(dt.isoformat())

def setup_logger(logger_name, log_file, level=logging.INFO):
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
  # # Decommentare per mandare l'output del logging a video
  # to_screen = logging.StreamHandler(sys.stdout)
  # logger.addHandler(to_screen)


def main(topic, msg, header):
  #  ************************************************************************************************************
  dirname, filename = os.path.split(os.path.abspath(__file__))

  ###
  # Gestione argomenti linea di comando
  ######################################
  msg_help  = '''Es.
       ./kafka_producer.py
  '''


  parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                   description='''KAFKA PRODUCER''',
                                   epilog=msg_help)

  parser.add_argument('-D','--debug',help='Modalita DEBUG',action='store_true' )
  args = parser.parse_args()


  # -------------------------------------------------------------------------------------------
  # Sezione Log FILE
  # -------------------------------------------------------------------------------------------
  if (args.debug):
    livello_log = logging.DEBUG
  else:
    livello_log = logging.INFO

  setup_logger('kafka_producer', dirname+"/log/kafka_producer.log", livello_log)
  logger        = logging.getLogger('kafka_producer')

  if not( os.path.isfile(dirname+"/config.cfg") ):
    logger.error("File di configurazione mancante")
    quit()

  logger.info('******* KAFKA PRODUCER *******')


  ###
  # LEGGO I PARAMETRI DI CONFIGURAZIONE
  #####################################
  cfg=get_config.get_config(dirname+"/config.cfg", logger)


  try:
      UUID=str(uuid.uuid4())
      header.append(('uuid', UUID.encode('utf-8')))
      
      ts=date_iso8601()
      header.append(('ts', ts.encode('utf-8')))

      producer = KafkaProducer(bootstrap_servers=cfg['kafka_bootstrap_servers'])
      data = json.dumps(msg).encode('utf-8')
      data = msg.encode('utf-8')
      

      future = producer.send(topic, value=data, headers=header)
      record_metadata = future.get(timeout=10)
      # Successful result returns assigned partition and offset
      logger.debug("Topic:     {}".format(record_metadata.topic))
      logger.debug("Partition: {}".format(record_metadata.partition))
      logger.debug("Offset:    {}".format(record_metadata.offset))



  except KafkaError as e:
      logger.error(str(e))

if __name__ == '__main__':
  test_data="sadsad dfsdf {'tag1': b'tag1'}"
  test_header=[('tag1', b'tag1'), ('tag2', b'tag2'), ('tag3', b'tag3')]

  main('audit', test_data, test_header)
