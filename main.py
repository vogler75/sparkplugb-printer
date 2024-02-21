import paho.mqtt.client as mqtt
import sparkplug.sparkplug_b as spb
import argparse

# Create the parser
parser = argparse.ArgumentParser(description="Sparkplug B MQTT client.")

# Add the arguments
parser.add_argument('-host', type=str, help='mqtt host to connect to', default="localhost")
parser.add_argument('-port', type=int, help='mqtt port to connect to', default=1883)
parser.add_argument('-username', type=str, help='mqtt username', default="")
parser.add_argument('-password', type=str, help='mqtt password', default="")
parser.add_argument('-topic', type=str, help='topic to subscribe to', default="spBv1.0/#")


# Parse the arguments
args = parser.parse_args()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("spBv1.0/#")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print("="*80)
    print("Topic: " + msg.topic)
    payload = spb.Payload()
    try:
        payload.ParseFromString(msg.payload)
        print(payload)
    except Exception as e:
        print("Error decoding: "+str(e)+ " message: "+str(msg.payload))


# Create an MQTT client instance
client = mqtt.Client()

# Set username and password
if args.username and args.username != "" and args.password and args.password != "":
    client.username_pw_set(args.username, args.password)

# Assign the on_connect and on_message callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
client.connect(args.host, args.port, 60)
client.loop_forever()
