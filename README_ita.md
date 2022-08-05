# MQTT2KAFKA
## _Gateway MQTT -> KAFKA_

[![N|Solid](https://www.python.org/static/img/python-logo.png)](https://www.python.org/)

[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)

MQTT2KAFKA è un microservizio che si sottoscrive ad un topic MQTT, arricchisce il messaggio acquisito con ulteriori metatag, e li inoltra ad un broker KAFKA.


## Funzionalità

- Il payload del messaggio MQTT viene acquisito e reinoltrato in binary string
- Viene aggiuto il seguente header:
```sh
  [('tag1', b'tag1'), 
   ('tag2', b'tag2'), 
   ('tag3', b'tag3'), 
   ('uuid', b'4eedd066-6eea-48d2-92a5-4ea308cf2286'), 
   ('ts', b'2022-07-25T18:54:27.823200+02:00')]
```



## Tech

MQTT2KAFKA è scritto in python ed ha come dipendenze le seguenti librerie:

- paho-mqtt - https://pypi.org/project/paho-mqtt/
- kafka-python - https://pypi.org/project/kafka-python/

Le restanti librerie sono quelle standard python
## Installazione

Copiare tutto il pacchetto in una directory.

Installare librerie python

```sh
pip3 install paho-mqtt
pip3 install kafka-python
```

## Configurazione

MQTT2KAFKA necessita della configurazione del file config.cfg
```sh
[MQTT]
mqtt_url= ["mqtt.server.cloud"]
#Parametro mqtt_mode: >> TLS | Vuoto
mqtt_mode=
mqtt_port= 8883
mqtt_topic= test_mqtt/#
mqtt_keepalive= 60
mqtt_user= *****
mqtt_password= *******
mqtt_transport= tcp
mqtt_qos= 0


[KAFKA]
topic               = kafka-topic
bootstrap_servers   = ["localhost:9092"]
auto_offset_reset   = earliest
```

## Esempio

Dopo aver impostato i parametri corretti nel file di configurazione config.cfg eseguire:
```sh
path/mqtt2kafka.py
```
è possibile abilitare la modalità DEBUG con il parametro -D
```sh
path/mqtt2kafka.py -D
```

a questo punto il microservizio è in esecuzione e si sottoscrive al topic MQTT in attesa di messaggi.
Quando riceve un messaggio lo impacchetta con gli altri metadati e lo inoltra al topic kafka configurato.

In modalità DEBUG, dopo l'avvio visualizza a terminale tutti i messaggi ricevuti via MQTT



Es.:
```sh
# Invio Messaggio MQTT
mosquitto_pub  -u "alfredo" -P "********" -t "test_mqtt" -m 'Test Messaggio ç°§^?=*/-- {"msg":"TEST"} FINE TEST MESSAGGIO'



# Ricezione messaggio KAFKA
Topic:                  audit
Partition:              0
Offset:                 107
Timestamp:              1658825117977
Key:                    None
Value:                  Test Messaggio ç°§^?=*/-- {"msg":"TEST"} FINE TEST MESSAGGIO
Headers:                [('tag1', b'tag1'), ('tag2', b'tag2'), ('tag3', b'tag3'), ('uuid', b'40136212-b1c6-4638-86aa-1a1800060183'), ('ts', b'2022-07-26T10:45:17.870988+02:00')]
Checksum:               None
Serialized key size:    -1
Serialized value size:  63
Serialized header size: 98
======================================================================

```




## ToDo

- Aggiungere la possibilità di parametrizzare i metadati a configurazione



## License

???
