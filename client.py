import json
import socket
import threading
import time
import logging

# logging setting
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(message)s\nraw:%(binary)s\ndecode:%(body)s', level=logging.INFO)


def packet(code: int, data: object):
    response = {
        "code": 0,
        "msg": "",
        "data": {
        }
    }
    if data:
        response["data"] = data
    # convert to string
    string_data = json.dumps(response)
    # get byte data
    byteData = string_data.encode("utf-8")
    size = (len(byteData)).to_bytes(2, byteorder='big')
    functionType = (code).to_bytes(2, byteorder='big')
    header = functionType + size
    fullPacket = header + byteData
    logging.info(' Message Sent:', extra={
                 'body': response, 'binary': fullPacket})
    return fullPacket


online_notice = packet(1, {
    "type": 2,
    "dev_name": "SN123",
    "num": 16,
    "signal": 90
})


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('3.81.118.208', 6720))
s.send(online_notice)


def send_keep_alive():
    while True:
        # sleep for 1 minutes/30s for debug
        time.sleep(30)
        # keep_alive -> packet(2, None)
        s.send(packet(2, None))


# start the child thread first
t = threading.Thread(target=send_keep_alive)
# set daemon
t.setDaemon(True)
t.start()


# main thread, listening to message
while True:
    msg = s.recv(1024)
    if msg:
      logging.info(' Message Received:', extra={'binary': msg, 'body': msg.decode('utf-8')})
      # decode message type
      ty = int.from_bytes(msg[0:2], byteorder='big')
      if ty != 1 and ty != 2:
        s.send(packet(ty, {"msg": "You tested with command"+str(ty)}))
      
