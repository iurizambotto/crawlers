
import json
from concurrent.futures import TimeoutError
# from google.cloud import pubsub_v1
# from google.auth import jwt
# from btp import BTP
# from dpw import DPW
# from sbs import SBS
from tcp import TCP
from itp import ITP
from nav import NAV
import configparser
import sys

config = configparser.RawConfigParser()
filename = "dev.properties"

config.read("./config/" + filename)

message = {}

exec = ITP()
# exec = TCP()
exec = NAV()
exec.exec_crawler(message=message, cfg=config)
