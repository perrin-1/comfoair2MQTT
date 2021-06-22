#!/usr/bin/env python3


# 3 Threads
# Comfoair Worker Thread (send comfoair commands) => get commands from commandQueue, put results in resultsQueue
# mqtt send thread (send cyclic data to mqtt) => periodic query of commands, publish results from resultsQueue
# mqtt receive thread (receive commands from mqtt) => put commands in commandQueue, publish results from resultsQueue

# 2 Queues
# commandQueue => commands to be sent to comfoair - object { 'id' : uuid, 'commands': [commandname1, commandname2], 'values': [value1, value2]}
# resultsQueue => contains results from commands, handled by mqtt send thread


import threading
import queue
import comfoair
import logging
import json
import sys
import signal
import uuid
from time import sleep
import paho.mqtt.client as mqtt
# import context  


serialport='/dev/ttyUSB0'
cyclic_commands = ['ReadComfortTemperature',
                'ReadSupplyAirPercentage','ReadBypassPercentage','ReadOperatingHoursAway',
                'ReadCurrentVentilationLevel', 'ReadFilterError', 'ReadP90State', 'ReadEWTLowTemperature' ]

mqtt_server = 'openhab.hs.cuh.hg.on2.de'
mqtt_port = 1883
mqtt_topic = '/comfoair/values'
mqtt_read_topic = '/comfoair/read'
mqtt_write_topic = '/comfoair/write/'

loglevel = logging.INFO
root = logging.getLogger()
root.setLevel(loglevel)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(loglevel)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
formatter = logging.Formatter('%(levelname)s - %(name)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

_stopThread = False

commandQueue = queue.Queue()
resultsQueue = queue.Queue()

sendInterval = 60

commandObject = { 'id' : 'uuid', 'commands' : []}
resultsObject = { 'id' : 'uuid', 'results' : ''}
threadLock = threading.Lock()
threads = []

def log_debug(text):    
    logging.getLogger('mqttComfoairDaemon').debug(text)

def log_info(text):    
    logging.getLogger('mqttComfoairDaemon').info(text)

def log_err(text):    
    logging.getLogger('mqttComfoairDaemon').error(text)
    
def signal_handler(sig, frame):
    log_info('Caught Signal {}. Stopping execution'.format(sig))
    global _stopThread
    _stopThread = True
    
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)




class Comfoair_Worker(threading.Thread):
    def __init__ (self, port, commandQueue, resultsQueue):
        super(Comfoair_Worker, self).__init__(name='Comfoair_Worker')
        self.logger = logging.getLogger('ComfoAir_Worker')
        global _stopThread
        self.port = port
        self.commandQueue = commandQueue
        self.resultsQueue = resultsQueue
        # we cache the results
        self.cacheResults = {}
        self.cacheResultsLen = 0
        
        
        self.kwl = comfoair.ComfoAir(serialport=self.port)
        self.log_info('Started Thread')
    
    def run (self):
        self.kwl.connect()
        self.kwl.run()
        while not _stopThread:
            sleep (1)
            if not self.commandQueue.empty():
                commandObject = self.commandQueue.get()
                self.log_debug('got commandobject: {}'.format(commandObject))  
                self.log_debug("Commandobject len = {}".format(len(commandObject['commands'])))          
                res = { 'id' :  str(commandObject['id']), 'results' : {}}
                for command in commandObject['commands']:
                    #Check if command is valid
                    if command in self.kwl._commandset:
                        if 'values' in commandObject:
                            value = commandObject['values'][0]
                        else:   
                            value = None
                        self.log_debug('Sending KWL Command {} with value {}'.format(command,value))            
                        kwlresult = None
                        try:
                            kwlresult = self.kwl.send_command(commandname=command, value=value)
                        except Exception as e:
                            self.log_err("Error sending Command {}: {}".format(command,e))
                        if kwlresult is not None:
                            res['results'].update(kwlresult)
                        #else:
                        #    res['results'].update(command , 'Error')
                        
                    else:
                        self.log_err('Ignoring unknown KWL Command {}'.format(command))      
                        #responses = {'error' : 'Ignoring unknown command {}'.format(command)}
                
                #at least one result
                if res['results']:
                    self.log_debug("resultsobject: {}".format(res))
                    self.log_debug("Resultsobject len = {}".format(len(res['results'])))    
                    if len(res['results']) >= self.cacheResultsLen:
                        self.cacheResultsLen = len(res['results'])
                        self.cacheResults = res['results']
                    else:
                        # Comfoair missed out some values
                        self.log_err("Number of Comfoair Results does not match expected numbers. Num Results {}, expected num results {}".format(len(res['results']),self.cacheResultsLen))
                        missingKeys = []
                        for key in self.cacheResults:
                            if key not in res['results']:
                                missingKeys.append(key)
                                res['results'][key] = self.cacheResults[key]
                        self.log_err ("Missing Key(s): {}. Adding cached value(s)".format(missingKeys))
                    self.resultsQueue.put(res)


        self.log_info('Disconnecting KWL')        
        self.kwl.disconnect()
        self.log_info('Stopping Thread') 
            
    def log_debug(self, text):    
        self.logger.debug(text)

    def log_info(self, text):    
        self.logger.info(text)

    def log_err(self, text):    
        self.logger.error(text)
    




class MQTT_worker(threading.Thread):
    def __init__ (self, commandQueue, resultsQueue, mqtt_server, mqtt_port, mqtt_topic, mqtt_read_topic, mqtt_write_topic):
            super(MQTT_worker, self).__init__(name='MQTT_worker')
            self.logger = logging.getLogger('MQTT_worker')
            global _stopThread
            self.name = 'MQTT_worker'
            self.commandQueue = commandQueue
            self.resultsQueue = resultsQueue
            self.mqtt_server = mqtt_server
            self.mqtt_port = mqtt_port
            self.mqtt_topic = mqtt_topic
            self.mqtt_read_topic = mqtt_read_topic
            self.mqtt_write_topic = mqtt_write_topic
            self.log_info('Started Thread')
    

    def run (self):
        self.mqtt_worker_inner = self.MQTT_worker_inner(commandQueue, resultsQueue, mqtt_server, mqtt_port, mqtt_topic, mqtt_read_topic, mqtt_write_topic)
        self.mqtt_worker_inner.run()
        


    def log_debug(self, text):    
            self.logger.debug(text)

    def log_info(self, text):    
        self.logger.info(text)

    def log_err(self, text):    
        self.logger.error(text)


    class MQTT_worker_inner(mqtt.Client):

        def __init__ (self, commandQueue, resultsQueue, mqtt_server, mqtt_port, mqtt_topic, mqtt_read_topic, mqtt_write_topic):
            super().__init__()
            self.logger = logging.getLogger('MQTT_worker')
            self.logger.setLevel(logging.DEBUG)
            global _stopThread
            self.commandQueue = commandQueue
            self.resultsQueue = resultsQueue
            self.mqtt_server = mqtt_server
            self.mqtt_port = mqtt_port
            self.mqtt_topic = mqtt_topic
            self.mqtt_read_topic = mqtt_read_topic
            self.mqtt_write_topic = mqtt_write_topic
            
            self.is_connected = False
            self.log_info('Started MQTT Worker Inner')
        

        def on_connect(self, mqttc, obj, flags, rc):
            self.log_debug("rc: "+str(rc))
            self.log_info("Connected to MQTT Server {}".format(self.mqtt_server))
            self.subscribe([(self.mqtt_read_topic, 0), (self.mqtt_write_topic+'#', 0)])
            self.is_connected=True
            

        def on_disconnect(self, mqttc, obj, flags, rc):
            self.log_err("Client got disconnected with rc={}. Try to reconnect...".format(rc))
            self.is_connected = False
            

        def on_message(self, mqttc, obj, msg):
            # Check Message
            self.log_debug(msg.topic+" "+str(msg.qos)+" "+str(msg.payload))
            
            # If Message Valid -> put into commandQueue
            if msg.topic == mqtt_read_topic:
                # Read Message. Add "Read" to Command and Publish
                # Command will be checked for sanity in Comfoair_Worker
                self.log_info("Putting Message into Queue {}".format(msg.topic))
                self.commandQueue.put({'id' : uuid.uuid1(), 'commands' : ['Read'+msg.payload.decode('utf-8')]})
            
            elif mqtt_write_topic in msg.topic or msg.topic == mqtt_write_topic:
                # Write Message. 
                command =''
                value = ''
                self.log_info("Handle write Message to topic {}".format(msg.topic))
                # Command will be checked for sanity in Comfoair_Worker
                # extract command and value
                # valid possibilities
                # a) /write/comforttemperature 20
                # b) /write/VentilationLevelLow
                # c) /write VentilationLevelLow
                # d) /write {"ComfortTemperature" : 20}
                
                # check if write topic contains extra chars a)+b)
                if msg.topic != mqtt_write_topic:
                    # Topic contains extra chars
                    topic = msg.topic.split('/')
                    command = 'Write'+topic[-1]
                    value = msg.payload.decode('utf-8')
                    self.log_debug("Extracted command={} value={}".format(command, value))
                else:
                    # Check if Payload is valid JSON d) else c)
                    try:
                        payload = json.loads(msg.payload.decode('utf-8'))
                        for k,v in payload.items():
                            self.log_debug("Putting DICT Message into Queue {} {}".format(k,v))
                            command = 'Write'+k
                            value = v
                    except ValueError:
                        command = msg.payload.decode('utf-8')

                if value is not None:
                    self.commandQueue.put({'id' : uuid.uuid1(), 'commands' : [str(command)], 'values' : [value]})
                else:
                    self.commandQueue.put({'id' : uuid.uuid1(), 'commands' : [str(command)]})
                    
            else:
                self.log_err("Unknown Message topic {}".format(msg.topic))
            
            

        def on_publish(self, mqttc, obj, mid):
            self.log_debug("mid: "+str(mid))

        def on_subscribe(self, mqttc, obj, mid, granted_qos):
            self.log_debug("Subscribed: "+str(mid)+" "+str(granted_qos))

        def on_log(self, mqttc, obj, level, string):
            self.logger.log(level, string)
            #print(string)

        def run(self):
            
            
            #self.subscribe(self.mqtt_write_topic, 0)
            # loop todo:
            # check incoming message, message sanity, put in commandQueue
            # check resultsQueue, if message, send mqtt


            rc = 0
            while not _stopThread:
                if not self.is_connected:
                    self.log_info ("Trying to connect to MQTT Server {}".format(self.mqtt_server))
                    try:
                        self.connect(self.mqtt_server, self.mqtt_port, 60)
                    except Exception as ex:
                        self.log_err("Unexpected error while connecting: {}".format(ex))
                        sleep (5)
                
                #self.log_debug('Looping')   
                rc = self.loop()
                if rc != 0 and self.is_connected:
                    # we should be connected so there should not be any error
                    self.log_err("Error: Got non zero return code from server: {}".format(rc))
                    self.is_connected = False
                if not self.resultsQueue.empty():
                    try:
                        resultsObject = self.resultsQueue.get()
                        self.log_debug('Got resultobject. Sending MQTT')            
                        #print (json.dumps(resultsObject,indent=4,sort_keys=True))
                        self.publish(self.mqtt_topic, json.dumps(resultsObject["results"]))
                    except queue.Empty:
                        continue
            
            self.log_info('Stopping Thread. RC={}'.format(rc)) 
            return rc
            

                
        def log_debug(self, text):    
            self.logger.debug(text)

        def log_info(self, text):    
            self.logger.info(text)

        def log_err(self, text):    
            self.logger.error(text)



class Cyclic_send(threading.Thread):
    def __init__ (self, commandQueue, sendInterval, cyclicCommands):
        super(Cyclic_send, self).__init__(name='Cyclic_send')
        self.logger = logging.getLogger('Cyclic_send')
        global _stopThread
        self.sendInterval = sendInterval
        self.commandQueue = commandQueue
        self.cyclicCommands = cyclicCommands
        self.log_info('Started Thread')
        
    
    def run (self):
        while not _stopThread:
            self.log_debug('Triggering cyclic commands')    
            self.commandQueue.put({'id' : uuid.uuid1(), 'commands' : self.cyclicCommands})
            for _ in range(0,self.sendInterval):
                sleep(1)
                #self.log_info('Sleep 1') 
                if _stopThread:
                    break
        self.log_info('Stopping Thread') 

        
            
    def log_debug(self, text):    
        self.logger.debug(text)

    def log_info(self, text):    
        self.logger.info(text)

    def log_err(self, text):    
        self.logger.error(text)
        




# Start MQTT Worker Thread
mqtt_worker = MQTT_worker(commandQueue=commandQueue, resultsQueue=resultsQueue, mqtt_server=mqtt_server, mqtt_port=mqtt_port, mqtt_topic=mqtt_topic, mqtt_read_topic=mqtt_read_topic, mqtt_write_topic=mqtt_write_topic)
mqtt_worker.start()
threads.append(mqtt_worker)

# Start Cyclic Command Sending Thread
cyclic_send = Cyclic_send(commandQueue=commandQueue, sendInterval=sendInterval, cyclicCommands=cyclic_commands)
cyclic_send.start()
threads.append(cyclic_send)



# Start Comfoair_Worker
worker = Comfoair_Worker(port=serialport, commandQueue=commandQueue, resultsQueue=resultsQueue)
worker.start()
threads.append(worker)

#Wait for stopThread
while not _stopThread:
    sleep (1)


# Wait for all threads to complete
for t in threads:
    log_info('Exiting {}'.format(t.name))    
    t.join()
    threads.remove(t)

log_info ("Exiting Main Thread")


