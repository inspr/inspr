import os
import json
import sys
from flask import Flask, request
from http import HTTPStatus
from .rest import *
from typing import Callable, Any

SIDECAR_READ_PORT = "INSPR_SCCLIENT_READ_PORT"
SIDECAR_WRITE_PORT = "INSPR_LBSIDECAR_WRITE_PORT"

class Client:
    def __init__(self) -> None:
        self.read_port = os.getenv(SIDECAR_READ_PORT)
        self.write_address = "http://localhost:" + str(os.getenv(SIDECAR_WRITE_PORT))
        self.app = Flask(__name__)

    def write_message(self, channel:str, msg) -> None:
        msg_body = {
            "data": msg
        }
        try:
            send_post_request(self.write_address + "/channel/" + channel, msg_body)
        except Exception as e:
            print(f"Error while trying to write message: {e}")
            raise Exception("failed to deliver message: channel: {}".format(channel))

    def handle_channel(self, channel:str) -> Callable[[Callable[[Any], Any]], Callable[[Any], Any]]:
        def wrapper(handle_func: Callable[[Any], Any]):
            def route_func():
                data = request.get_json(force=True)
                try:
                    handle_func(data["data"])
                except:
                    err = "Error handling message"
                    return err, HTTPStatus.INTERNAL_SERVER_ERROR

                return '', HTTPStatus.OK

            self.app.add_url_rule("/channel/" + channel, endpoint = channel, view_func = route_func, methods=["POST"])
            return handle_func
        return wrapper
    
    def handle_route(self, path:str) -> Callable[[Callable[[Any], Any]], Callable[[Any], Any]]:
        def wrapper(handle_func: Callable[[Any], Any]):
            route = remove_prefix(path, '/')
            self.app.add_url_rule("/route/" + route, endpoint = "/route/" + route, view_func = handle_func, methods=["GET", "DELETE", "POST", "PUT"])
            return handle_func
        return wrapper

    def send_request(self, node_name:str, path:str, method:str, body) -> Response:
        try:
            url = self.write_address + "/route/" + node_name + "/" + path
            resp = send_new_request(url, method, body)
            return resp
        
        except Exception as e:
            print(f"Error while trying to send request: {e}")
            raise Exception("failed to deliver message: route: {}".format(url))

    def run(self) -> None:
        links = []
        for rule in self.app.url_map.iter_rules():
            links.append(rule.endpoint)
        print("registered routes =", links, file=sys.stderr)

        self.app.run(port=self.read_port)


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text