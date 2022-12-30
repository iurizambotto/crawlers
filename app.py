from tcp import TCP
import configparser

config = configparser.RawConfigParser()
filename = "dev.properties"

config.read("./config/" + filename)

message = {}

exec = TCP()
exec.exec_crawler(message=message, cfg=config)
