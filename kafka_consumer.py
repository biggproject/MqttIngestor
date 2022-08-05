#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Author: alfredo
# @Date:   25-07-2022 11:36:33
# @Last Modified by:   alfredo
# @Last Modified time: 2022-07-26 10:46:18

import os
import sys
import configparser
import argparse


# CHECK LOCK
import signal, errno
from contextlib import contextmanager
import fcntl

import logging
from logging import handlers, Formatter
import traceback

import get_config
from kafka import KafkaConsumer


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


def setup_logger(logger_name, log_file, level=logging.INFO):
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



#  ************************************************************************************************************
dirname, filename = os.path.split(os.path.abspath(__file__))
hostname = os.uname()[1]
miopid   = os.getpid()
ct       = 0

###
# Gestione argomenti linea di comando
######################################
msg_help  = '''Es.
     ./kafka_consumer.py
'''


parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                 description='''KAFKA''',
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


lock_file     = '/tmp/kafka_consumer.lck'
setup_logger('kafka_consumer', dirname+"/log/kafka_consumer.log", livello_log)
logger        = logging.getLogger('kafka_consumer')



if not( os.path.isfile(dirname+"/config.cfg") ):
  logger.error("File di configurazione mancante")
  quit()




logger.info('******* KAFKA CONSUMER *******')


# verifico se esiste altro processo in corso
check_lock()

###
# LEGGO I PARAMETRI DI CONFIGURAZIONE
#####################################
cfg=get_config.get_config(dirname+"/config.cfg", logger)


try:
  consumer = KafkaConsumer(cfg['kafka_topic'], 
                           bootstrap_servers=cfg['kafka_bootstrap_servers'],
                           auto_offset_reset=cfg['kafka_auto_offset_reset'],)
except Exception as e:
  logger.error("Connessione KAFKA non riuscita ")
  logger.error(e)
  quit()


logger.info("Connessione KAFKA OK... In attesa di messaggi... ")
for message in consumer:
  print("Topic:                  {}".format(message.topic))
  print("Partition:              {}".format(message.partition))
  print("Offset:                 {}".format(message.offset))
  print("Timestamp:              {}".format(message.timestamp))
  print("Key:                    {}".format(message.key))
  print("Value:                  {}".format(message.value.decode('utf-8')))
  print("Headers:                {}".format(message.headers))
  print("Checksum:               {}".format(message.checksum))
  print("Serialized key size:    {}".format(message.serialized_key_size))
  print("Serialized value size:  {}".format(message.serialized_value_size))
  print("Serialized header size: {}".format(message.serialized_header_size))
  print("======================================================================")

  # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
  #                                       message.offset, message.key,
  #                                       message.value.decode('utf-8')))
